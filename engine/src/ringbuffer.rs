// A single-producer / single-consumer, lock-free ring buffer -- a minimal
// version of the core idea behind the LMAX Disruptor: pre-allocate a fixed
// array once, never allocate on the hot path again, and use plain atomic
// counters (not a mutex, not even a CAS loop, since there's exactly one
// producer and one consumer) with the minimum memory ordering that is
// actually correct.
//
// Design notes:
//
// - `head` is written only by the consumer and read only by the producer;
//   `tail` is the mirror image. Each side treats its OWN counter with
//   `Relaxed` (nobody else ever writes it) and the OTHER side's counter with
//   `Acquire` on load / `Release` on store. That's exactly enough to
//   establish a happens-before edge for the slot data without paying for a
//   full SeqCst fence -- SeqCst buys you a single total order across
//   *multiple independent* atomics, which nothing here needs, since there is
//   only one producer-consumer edge in this whole data structure.
//
// - `head` and `tail` are cache-line padded (64 bytes) so a producer write to
//   `tail` doesn't invalidate the cache line the consumer is spinning on
//   while reading `head`, and vice versa. Without this, "false sharing" turns
//   two logically independent counters into a shared bottleneck they have no
//   business being -- this is the single most-cited real-world win from the
//   Disruptor paper.
//
// - Slots are `MaybeUninit<T>` so we never need a default/zero value for T
//   and never run a destructor on memory that was never initialised.
//
// - `T: Copy` is a constraint, not laziness: hot-path messages should be
//   plain, fixed-size data with no heap indirection, so pushing/popping never
//   touches the allocator and there is no Drop glue to reason about.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

struct RingInner<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize, // always a power of two
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    dropped: CachePadded<AtomicUsize>,
}

// Safety: the only mutable access to a slot happens through the disciplined
// head/tail protocol in `try_push`/`try_pop` below, which never lets the
// producer and consumer touch the same slot at the same time. T: Copy means
// there is no Drop impl to race on.
unsafe impl<T: Copy + Send> Sync for RingInner<T> {}
unsafe impl<T: Copy + Send> Send for RingInner<T> {}

fn make_inner<T: Copy>(capacity: usize) -> RingInner<T> {
    let capacity = capacity.next_power_of_two().max(2);
    let mut v = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        v.push(UnsafeCell::new(MaybeUninit::uninit()));
    }
    RingInner {
        buf: v.into_boxed_slice(),
        capacity,
        mask: capacity - 1,
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        dropped: CachePadded(AtomicUsize::new(0)),
    }
}

pub struct Producer<T: Copy> {
    inner: Arc<RingInner<T>>,
}
pub struct Consumer<T: Copy> {
    inner: Arc<RingInner<T>>,
}

pub fn channel<T: Copy>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let inner = Arc::new(make_inner(capacity));
    (
        Producer {
            inner: inner.clone(),
        },
        Consumer { inner },
    )
}

impl<T: Copy> Producer<T> {
    /// Attempts to publish `value`. Returns `Err(value)` if the consumer
    /// hasn't kept up and the buffer is full, rather than blocking -- a real
    /// feed handler faces the same choice under backpressure, and we choose
    /// drop-and-count over stalling the hot path.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let inner = &*self.inner;
        let tail = inner.tail.load(Ordering::Relaxed);
        let head = inner.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= inner.capacity {
            inner.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(value);
        }
        let idx = tail & inner.mask;
        unsafe {
            (*inner.buf[idx].get()).write(value);
        }
        inner.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn dropped_count(&self) -> usize {
        self.inner.dropped.load(Ordering::Relaxed)
    }
}

impl<T: Copy> Consumer<T> {
    pub fn try_pop(&self) -> Option<T> {
        let inner = &*self.inner;
        let head = inner.head.load(Ordering::Relaxed);
        let tail = inner.tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head & inner.mask;
        let value = unsafe { (*inner.buf[idx].get()).assume_init_read() };
        inner.head.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    pub fn dropped_count(&self) -> usize {
        self.inner.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn single_threaded_fifo_order() {
        let (p, c) = channel::<u64>(8);
        for i in 0..5 {
            p.try_push(i).unwrap();
        }
        for i in 0..5 {
            assert_eq!(c.try_pop(), Some(i));
        }
        assert_eq!(c.try_pop(), None);
    }

    #[test]
    fn reports_full_without_blocking() {
        let (p, _c) = channel::<u64>(4);
        for i in 0..4 {
            assert!(p.try_push(i).is_ok());
        }
        assert!(p.try_push(999).is_err());
        assert_eq!(p.dropped_count(), 1);
    }

    #[test]
    fn concurrent_producer_consumer_preserves_order_and_completeness() {
        const N: u64 = 100_000;
        let (p, c) = channel::<u64>(1024);
        let producer = thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                if p.try_push(i).is_ok() {
                    i += 1;
                }
            }
        });
        let consumer = thread::spawn(move || {
            let mut expected = 0u64;
            while expected < N {
                if let Some(v) = c.try_pop() {
                    assert_eq!(v, expected, "values must arrive in strict FIFO order");
                    expected += 1;
                }
            }
            expected
        });
        producer.join().unwrap();
        let received = consumer.join().unwrap();
        assert_eq!(received, N);
    }
}
