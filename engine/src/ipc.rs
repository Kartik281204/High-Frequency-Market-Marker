// A small TCP server: publishes newline-delimited JSON snapshots to any
// connected monitor (the Python/WebSocket relay) at a fixed rate, and listens
// on the same connection for a "KILL\n" line to trip the manual kill switch.
//
// Deliberately plain blocking std TCP, no async runtime -- because this is
// explicitly NOT on the trading hot path. It's the monitoring sidecar, and
// production systems keep this kind of thing well away from anything
// latency-sensitive for exactly that reason: you do not want a JSON
// serialization call or a socket write on the same thread that is deciding
// whether to requote.

use serde::Serialize;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Serialize, Clone, Default)]
pub struct Snapshot {
    pub ts_ms: u64,
    pub mid: f64,
    pub our_bid: Option<f64>,
    pub our_ask: Option<f64>,
    pub inventory: i64,
    pub cash: f64,
    pub unrealized_pnl: f64,
    pub sigma: f64,
    pub var_95: f64,
    pub es_95: f64,
    pub killed: bool,
    pub kill_reason: Option<String>,
    pub bid_depth: Vec<(f64, u64)>,
    pub ask_depth: Vec<(f64, u64)>,
    pub trades_count: u64,
    pub feed_packets_received: u64,
    pub feed_gaps_detected: u64,
    pub feed_events_lost: u64,
    pub ring_dropped: usize,
}

pub fn run_server(
    addr: &str,
    snapshot: Arc<Mutex<Snapshot>>,
    manual_kill: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    publish_interval: Duration,
) {
    let listener = match TcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[ipc] failed to bind {addr}: {e}");
            return;
        }
    };
    listener
        .set_nonblocking(true)
        .expect("failed to set listener nonblocking");
    println!("[ipc] snapshot server listening on {addr}");

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, peer)) => {
                println!("[ipc] monitor connected from {peer}");
                let snap = snapshot.clone();
                let kill = manual_kill.clone();
                let sd = shutdown.clone();
                thread::spawn(move || handle_client(stream, snap, kill, sd, publish_interval));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                eprintln!("[ipc] accept error: {e}");
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn handle_client(
    stream: std::net::TcpStream,
    snapshot: Arc<Mutex<Snapshot>>,
    manual_kill: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    publish_interval: Duration,
) {
    let reader_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[ipc] failed to clone stream: {e}");
            return;
        }
    };
    let sd_reader = shutdown.clone();
    thread::spawn(move || {
        let mut reader = BufReader::new(reader_stream);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break, // client disconnected
                Ok(_) => {
                    if line.trim().eq_ignore_ascii_case("KILL") {
                        println!("[ipc] manual KILL received from monitor");
                        manual_kill.store(true, Ordering::Relaxed);
                    }
                }
                Err(_) => break,
            }
            if sd_reader.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    let mut writer = stream;
    while !shutdown.load(Ordering::Relaxed) {
        let payload = {
            let guard = snapshot.lock().unwrap();
            serde_json::to_string(&*guard).unwrap_or_default()
        };
        if writer.write_all(payload.as_bytes()).is_err() {
            break;
        }
        if writer.write_all(b"\n").is_err() {
            break;
        }
        thread::sleep(publish_interval);
    }
}
