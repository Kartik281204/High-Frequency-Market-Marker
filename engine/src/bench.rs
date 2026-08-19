// Latency and throughput benchmark for the lock-free ring buffer, isolated
// from the rest of the simulation (no UDP, no OS-scheduled sleeps) so the
// numbers reflect the ring buffer itself, not simulation-loop pacing.
//
// Run with: ./mm_engine --bench <n_messages>
//
// A note on methodology that matters more than it might look: pure
// spin-waiting (`std::hint::spin_loop()`) on a full/empty buffer is only a
// valid latency-measurement strategy when producer and consumer genuinely
// run in parallel on separate cores. On a single-core host, two threads that
// both spin are fighting each other for the only CPU; the OS scheduler can
// only switch between them at its normal preemption granularity (often
// single-digit milliseconds), and the resulting "latency" mostly measures
// that scheduling quantum, not the ring buffer. This benchmark checks
// `std::thread::available_parallelism()` and only spins if it can actually
// get true parallelism; otherwise it yields, which is the honest choice
// given the hardware it's actually running on, not a flattering one.
//
// Separately: this measures whatever container/VM happens to be running it,
// not dedicated, core-pinned, isolated hardware -- so even the multi-core
// numbers here are not "real HFT" latency figures, and shouldn't be quoted
// as such. What IS real is the measurement methodology: true one-way
// publish-to-consume latency from a monotonic clock embedded in each
// message and read back out on the other side. The same harness on tuned,
// multi-core, isolated hardware would give you the real number.

use crate::ringbuffer;
use std::thread;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
struct BenchMsg {
    published_at: Instant,
}

fn wait_hint(can_truly_parallelize: bool) {
    if can_truly_parallelize {
        std::hint::spin_loop();
    } else {
        thread::yield_now();
    }
}

/// Push-then-immediately-pop on a single thread, back to back, n times. No
/// second thread is involved at all, so this number is not affected by core
/// count or OS scheduling -- it isolates the ring buffer's own mechanical
/// cost (an atomic load, an atomic store, and a fixed-size memory copy in
/// each direction) from the cross-thread handoff cost measured by `run()`.
/// On a single-core host this is the more informative of the two numbers.
fn single_threaded_roundtrip_ns(n: u64) -> Vec<u64> {
    let (tx, rx) = ringbuffer::channel::<BenchMsg>(64);
    let mut costs_ns = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let t0 = Instant::now();
        tx.try_push(BenchMsg { published_at: t0 }).expect("single-threaded push cannot fail");
        rx.try_pop().expect("just-pushed value must be poppable");
        costs_ns.push(t0.elapsed().as_nanos() as u64);
    }
    costs_ns
}

fn print_latency_summary(label: &str, mut latencies_ns: Vec<u64>) {
    latencies_ns.sort_unstable();
    let n = latencies_ns.len();
    let pct = |p: f64| latencies_ns[((n as f64 - 1.0) * p).round() as usize];
    let mean_ns = latencies_ns.iter().sum::<u64>() as f64 / n as f64;
    println!("{label}");
    println!("  min:    {:>8} ns", latencies_ns[0]);
    println!("  mean:   {:>8.0} ns", mean_ns);
    println!("  p50:    {:>8} ns", pct(0.50));
    println!("  p99:    {:>8} ns", pct(0.99));
    println!("  p99.9:  {:>8} ns", pct(0.999));
    println!("  max:    {:>8} ns", latencies_ns[n - 1]);
}

pub fn run(n_messages: u64) {
    let capacity = 8192usize;
    let (tx, rx) = ringbuffer::channel::<BenchMsg>(capacity);

    let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let can_truly_parallelize = cores >= 2;

    let start = Instant::now();

    let producer = thread::spawn(move || {
        for _ in 0..n_messages {
            let msg = BenchMsg { published_at: Instant::now() };
            loop {
                match tx.try_push(msg) {
                    Ok(()) => break,
                    Err(_) => wait_hint(can_truly_parallelize),
                }
            }
        }
        tx.dropped_count() // count of retried (not lost) pushes -- see below
    });

    let consumer = thread::spawn(move || {
        let mut latencies_ns: Vec<u64> = Vec::with_capacity(n_messages as usize);
        let mut received = 0u64;
        while received < n_messages {
            match rx.try_pop() {
                Some(msg) => {
                    latencies_ns.push(msg.published_at.elapsed().as_nanos() as u64);
                    received += 1;
                }
                None => wait_hint(can_truly_parallelize),
            }
        }
        latencies_ns
    });

    let backpressure_retries = producer.join().expect("producer thread panicked");
    let latencies_ns = consumer.join().expect("consumer thread panicked");
    let elapsed = start.elapsed();

    let n = latencies_ns.len();
    let throughput = n_messages as f64 / elapsed.as_secs_f64();

    println!("=== ring buffer benchmark (SPSC, capacity {capacity}) ===");
    println!(
        "cores available: {cores} ({})",
        if can_truly_parallelize {
            "spin-waiting -- true parallelism available"
        } else {
            "yielding, not spinning -- single core, see note in bench.rs"
        }
    );
    println!("environment: this sandbox's container -- not dedicated/pinned hardware, see README");
    println!("messages:            {n_messages}");
    println!("wall time:           {:.3} s", elapsed.as_secs_f64());
    println!("throughput:          {:>12.0} msgs/sec", throughput);
    println!(
        "backpressure retries:{:>13} (every failed try_push increments this, whether \
         spinning or yielding; nothing is actually lost, since the caller retries -- \
         it measures contention, not data loss)",
        backpressure_retries
    );
    println!();
    print_latency_summary(
        "cross-thread publish -> consume latency (producer thread A, consumer thread B):",
        latencies_ns,
    );
    if !can_truly_parallelize {
        println!(
            "  ^ on this single-core host, these numbers are dominated by OS scheduler\n\
             \x20   preemption granularity (thread B can only make progress once the OS\n\
             \x20   deschedules thread A), not by the ring buffer itself. See the\n\
             \x20   single-threaded number below for that."
        );
    }
    println!();
    let n_rt = n.min(200_000).max(1000) as u64; // keep this part quick even for huge --bench values
    print_latency_summary(
        &format!("single-threaded push+pop round-trip cost ({n_rt} iterations, no second thread involved):"),
        single_threaded_roundtrip_ns(n_rt),
    );
}
