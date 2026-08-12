// The feed handler. This is the piece that, in a real system, would sit
// closest to the NIC: bind a socket, receive the exchange's multicast market
// data feed, and hand normalized events off to the strategy at whatever rate
// it can consume them -- decoupled via the lock-free ring buffer so a slow
// strategy tick never causes a dropped datagram.
//
// It also does the one thing every real feed handler has to do that a naive
// implementation skips: detect gaps in the sequence numbers. UDP is
// unreliable and unordered by design, so a production book-builder tracks
// the expected next sequence number and, on a gap, would normally request a
// snapshot recovery from the exchange. This project doesn't implement full
// snapshot recovery (that's a real subsystem in its own right -- see the
// README), but it does detect and count gaps, which is the health metric
// that actually matters operationally: if this number is climbing, your
// book view is silently drifting from the truth.

use crate::ringbuffer::Producer;
use crate::types::{FeedPacket, SeqEvent};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub struct FeedHealth {
    pub packets_received: AtomicU64,
    pub gaps_detected: AtomicU64,
    pub events_lost_to_gaps: AtomicU64,
}

impl FeedHealth {
    pub fn new() -> Self {
        Self {
            packets_received: AtomicU64::new(0),
            gaps_detected: AtomicU64::new(0),
            events_lost_to_gaps: AtomicU64::new(0),
        }
    }
}

pub fn run(
    bind_addr: &str,
    out: Producer<SeqEvent>,
    health: Arc<FeedHealth>,
    shutdown: Arc<AtomicBool>,
) {
    let socket = UdpSocket::bind(bind_addr).expect("failed to bind UDP feed listener");
    socket
        .set_read_timeout(Some(std::time::Duration::from_millis(100)))
        .expect("failed to set UDP read timeout");

    let mut expected_seq: Option<u64> = None;
    let mut buf = [0u8; 2048];

    while !shutdown.load(Ordering::Relaxed) {
        let (n, _peer) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(_) => continue,
        };

        let packet: FeedPacket = match serde_json::from_slice(&buf[..n]) {
            Ok(p) => p,
            Err(_) => continue, // malformed datagram; drop it, don't crash the handler
        };

        health.packets_received.fetch_add(1, Ordering::Relaxed);

        match expected_seq {
            None => {
                expected_seq = Some(packet.seq + 1);
            }
            Some(exp) => {
                if packet.seq >= exp {
                    if packet.seq > exp {
                        let lost = packet.seq - exp;
                        health.gaps_detected.fetch_add(1, Ordering::Relaxed);
                        health.events_lost_to_gaps.fetch_add(lost, Ordering::Relaxed);
                    }
                    expected_seq = Some(packet.seq + 1);
                }
                // else: packet.seq < exp -- a stale/duplicate/reordered
                // datagram. The event is still forwarded below (better to
                // process a late arrival than silently drop it), but we must
                // not rewind our expectation of what comes next.
            }
        }

        out.try_push(SeqEvent { seq: packet.seq, event: packet.event }).ok();
    }
}
