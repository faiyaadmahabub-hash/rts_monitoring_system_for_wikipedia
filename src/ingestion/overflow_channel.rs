// ingestion/overflow_channel.rs — Drop-oldest bounded channel (Component A).
// Backpressure management: when full, drops the oldest item and logs overflow.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Instant;

use crate::metrics::collector::MetricsCollector;

pub struct OverflowChannel {
    inner: Arc<ChannelInner>,
}

struct ChannelInner {
    buffer: Mutex<VecDeque<(String, Instant)>>,
    capacity: usize,
    condvar: Condvar,
    metrics: MetricsCollector,
}

impl OverflowChannel {
    /// Creates a bounded channel with the given capacity and metrics collector.
    pub fn new(capacity: usize, metrics: MetricsCollector) -> Self {
        Self {
            inner: Arc::new(ChannelInner {
                buffer: Mutex::new(VecDeque::with_capacity(capacity)),
                capacity,
                condvar: Condvar::new(),
                metrics,
            }),
        }
    }

    /// Returns a cloneable sender handle for this channel.
    pub fn sender(&self) -> OverflowSender {
        OverflowSender { inner: self.inner.clone() }
    }

    /// Returns a receiver handle for this channel.
    pub fn receiver(&self) -> OverflowReceiver {
        OverflowReceiver { inner: self.inner.clone() }
    }

    /// Returns current number of items in the channel buffer.
    pub fn len(&self) -> usize {
        self.inner.buffer.lock().unwrap().len()
    }
}

#[derive(Clone)]
pub struct OverflowSender {
    inner: Arc<ChannelInner>,
}

impl OverflowSender {
    /// Sends an item. If at capacity, drops the oldest item and records an overflow.
    pub fn send(&self, item: (String, Instant)) {
        let mut buf = self.inner.buffer.lock().unwrap();

        if buf.len() >= self.inner.capacity {
            let _dropped = buf.pop_front();
            self.inner.metrics.record_overflow();
        }

        buf.push_back(item);
        self.inner.condvar.notify_one();
    }
}

pub struct OverflowReceiver {
    inner: Arc<ChannelInner>,
}

impl OverflowReceiver {
    /// Non-blocking receive. Returns None if the channel is empty.
    pub fn try_recv(&self) -> Option<(String, Instant)> {
        self.inner.buffer.lock().unwrap().pop_front()
    }

    /// Blocking receive. Waits on condvar until an item is available.
    pub fn recv(&self) -> Option<(String, Instant)> {
        let mut buf = self.inner.buffer.lock().unwrap();
        while buf.is_empty() {
            buf = self.inner.condvar.wait(buf).unwrap();
        }
        buf.pop_front()
    }

    /// Returns current number of items in the channel buffer.
    pub fn len(&self) -> usize {
        self.inner.buffer.lock().unwrap().len()
    }
}
