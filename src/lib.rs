use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;
use std::num::NonZeroU64;

#[derive(Debug)]
pub struct WeightedFairQueue<K, T> {
    items: BinaryHeap<HeapItem<K, T>>,
    overflow: BinaryHeap<OverflowHeapItem<K, T>>,
    flows: HashMap<K, FlowState>,
    queue_size: QueueSize,
    max_normal_queue_size: usize,
    virtual_time: u64,
    seqno: u64,
}

impl<K, T> WeightedFairQueue<K, T>
where
    K: Clone + Eq + Hash,
    T: AsRef<[u8]>,
{
    pub fn new(max_normal_queue_size: usize) -> Self {
        Self {
            items: BinaryHeap::new(),
            overflow: BinaryHeap::new(),
            flows: HashMap::new(),
            queue_size: QueueSize::new(),
            max_normal_queue_size,
            virtual_time: 0,
            seqno: 0,
        }
    }

    pub fn enqueue(&mut self, item: Item<K, T>) {
        if !self.flows.contains_key(item.flow_key()) {
            let flow = FlowState {
                last_virtual_finish_time: self.virtual_time,
                queue_size: QueueSize::new(),
            };
            self.flows.insert(item.flow_key().clone(), flow);
        }
        let item_size = item.data_size();

        let flow = self.flows.get_mut(item.flow_key()).expect("unreachable");
        flow.last_virtual_finish_time += item_size as u64 * item.weight.get();

        let item = HeapItem {
            inner: item,
            seqno: self.seqno,
            virtual_finish_time: flow.last_virtual_finish_time,
        };
        self.seqno += 1;

        if self.queue_size.normal + item_size > self.max_normal_queue_size {
            flow.queue_size.overflow += item_size;
            self.queue_size.overflow += item_size;
            self.overflow.push(OverflowHeapItem(item));
        } else {
            flow.queue_size.normal += item_size;
            self.queue_size.normal += item_size;
            self.items.push(item);
        }
    }

    pub fn dequeue(&mut self) -> Option<Item<K, T>> {
        let item = if let Some(item) = self.items.pop() {
            item
        } else {
            return None;
        };

        self.virtual_time = item.virtual_finish_time;
        self.queue_size.normal -= item.inner.data_size();

        let flow = self
            .flows
            .get_mut(item.inner.flow_key())
            .expect("unreachable");
        flow.queue_size.normal -= item.inner.data_size();
        if flow.queue_size.normal == 0 {
            self.flows.remove(item.inner.flow_key());
        }

        while let Some(next) = self.overflow.pop() {
            if self.queue_size.normal + next.0.inner.data_size() > self.max_normal_queue_size {
                self.overflow.push(next);
                break;
            }

            let flow = self
                .flows
                .get_mut(item.inner.flow_key())
                .expect("unreachable");
            flow.queue_size.normal += next.0.inner.data_size();
            flow.queue_size.overflow -= next.0.inner.data_size();

            self.queue_size.normal += next.0.inner.data_size();
            self.queue_size.overflow -= next.0.inner.data_size();

            self.items.push(next.0);
        }

        Some(item.inner)
    }

    pub fn queue_size(&self) -> QueueSize {
        self.queue_size.clone()
    }

    pub fn flows(&self) -> &HashMap<K, FlowState> {
        &self.flows
    }
}

#[derive(Debug, Clone)]
pub struct Item<K, T> {
    flow_key: K,
    weight: NonZeroU64,
    data: T,
}

impl<K, T> Item<K, T> {
    pub fn new(flow_key: K, weight: NonZeroU64, data: T) -> Self {
        Self {
            flow_key,
            weight,
            data,
        }
    }

    pub fn flow_key(&self) -> &K {
        &self.flow_key
    }

    pub fn weight(&self) -> NonZeroU64 {
        self.weight
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn into_data(self) -> T {
        self.data
    }
}

impl<K, T> Item<K, T>
where
    T: AsRef<[u8]>,
{
    fn data_size(&self) -> usize {
        self.data.as_ref().len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IpPrecedence {
    P0,
    P1,
    P2,
    P3,
    P4,
    P5,
    P6,
    P7,
}

impl IpPrecedence {
    pub fn weight(self) -> NonZeroU64 {
        let w = match self {
            Self::P0 => 32768,
            Self::P1 => 16384,
            Self::P2 => 10920,
            Self::P3 => 8192,
            Self::P4 => 6552,
            Self::P5 => 5456,
            Self::P6 => 4680,
            Self::P7 => 4096,
        };
        NonZeroU64::new(w).expect("unreachable")
    }
}

#[derive(Debug, Clone)]
pub struct FlowState {
    pub queue_size: QueueSize,
    pub last_virtual_finish_time: u64,
}

#[derive(Debug, Clone)]
pub struct QueueSize {
    normal: usize,
    overflow: usize,
}

impl QueueSize {
    fn new() -> Self {
        Self {
            normal: 0,
            overflow: 0,
        }
    }

    pub fn total(&self) -> usize {
        self.normal + self.overflow
    }

    pub fn normal(&self) -> usize {
        self.normal
    }

    pub fn overflow(&self) -> usize {
        self.overflow
    }
}

#[derive(Debug, Clone)]
struct HeapItem<K, T> {
    inner: Item<K, T>,
    seqno: u64,
    virtual_finish_time: u64,
}

impl<K, T> PartialEq for HeapItem<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.seqno == other.seqno
    }
}

impl<K, T> Eq for HeapItem<K, T> {}

impl<K, T> PartialOrd for HeapItem<K, T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, T> Ord for HeapItem<K, T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.virtual_finish_time
            .cmp(&other.virtual_finish_time)
            .reverse()
    }
}

#[derive(Debug, Clone)]
struct OverflowHeapItem<K, T>(HeapItem<K, T>);

impl<K, T> PartialEq for OverflowHeapItem<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K, T> Eq for OverflowHeapItem<K, T> {}

impl<K, T> PartialOrd for OverflowHeapItem<K, T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, T> Ord for OverflowHeapItem<K, T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .inner
            .weight()
            .cmp(&other.0.inner.weight())
            .then_with(|| self.0.seqno.cmp(&other.0.seqno).reverse())
    }
}
