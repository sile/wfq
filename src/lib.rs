use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;
use std::num::{NonZeroU64, NonZeroU8};

const SCALED_ONE: u64 = 1 << 16;

#[derive(Debug)]
pub struct WeightedFairQueue<K, T> {
    items: BinaryHeap<HeapItem<K, T>>,
    overflow: BinaryHeap<OverflowHeapItem<K, T>>,
    flows: HashMap<K, FlowInfo>,
    queue_size: usize,
    max_queue_size: usize,
    virtual_time: u64,
    weight_sum: u64,
    seqno: u64,
}

impl<K, T> WeightedFairQueue<K, T>
where
    K: Clone + Eq + Hash,
    T: AsRef<[u8]>,
{
    pub fn new(max_queue_size: usize) -> Self {
        Self {
            items: BinaryHeap::new(),
            overflow: BinaryHeap::new(),
            flows: HashMap::new(),
            queue_size: 0,
            max_queue_size,
            virtual_time: 0,
            weight_sum: 0,
            seqno: 0,
        }
    }

    pub fn enqueue(&mut self, item: Item<K, T>) {
        if !self.flows.contains_key(item.flow_key()) {
            let fi = FlowInfo {
                last_virtual_finish_time: self.virtual_time,
                weight: item.flow_weight(),
                size: 0,
            };
            self.weight_sum += u64::from(fi.weight.get());
            self.flows.insert(item.flow_key().clone(), fi);
        }
        let fi = self.flows.get_mut(item.flow_key()).expect("unreachable");

        let data_size = item.data().as_ref().len();
        let item = HeapItem {
            item,
            seqno: self.seqno,
            virtual_finish_time: fi.last_virtual_finish_time + data_size as u64 * fi.inv_w(),
        };
        self.seqno += 1;

        fi.last_virtual_finish_time = item.virtual_finish_time;
        fi.size += item.data_size();

        if self.queue_size + item.data_size() > self.max_queue_size {
            let ohi = OverflowHeapItem(item);
            self.overflow.push(ohi);
        } else {
            self.queue_size += item.data_size();
            self.items.push(item);
        }
    }

    pub fn dequeue(&mut self) -> Option<Item<K, T>> {
        let item = if let Some(item) = self.items.pop() {
            item
        } else {
            return None;
        };

        self.virtual_time += item.data_size() as u64 * self.inv_weight_sum();
        self.queue_size -= item.data_size();

        let fi = self
            .flows
            .get_mut(item.item.flow_key())
            .expect("unreachable");
        fi.size -= item.data_size();
        if fi.size == 0 {
            self.weight_sum -= u64::from(fi.weight.get());
            self.flows.remove(item.item.flow_key());
        }

        while let Some(next) = self.overflow.pop() {
            if self.queue_size + next.0.data_size() > self.max_queue_size {
                self.overflow.push(next);
                break;
            }

            self.queue_size += next.0.data_size();
            self.items.push(next.0);
        }

        Some(item.item)
    }

    fn inv_weight_sum(&self) -> u64 {
        SCALED_ONE / self.weight_sum
    }
}

#[derive(Debug, Clone)]
pub struct Item<K, T> {
    flow_key: K,
    flow_weight: NonZeroU8,
    data: T,
}

impl<K, T> Item<K, T> {
    pub fn new(flow_key: K, flow_weight: NonZeroU8, data: T) -> Self {
        Self {
            flow_key,
            flow_weight,
            data,
        }
    }

    pub fn flow_key(&self) -> &K {
        &self.flow_key
    }

    pub fn flow_weight(&self) -> NonZeroU8 {
        self.flow_weight
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn into_data(self) -> T {
        self.data
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
struct HeapItem<K, T> {
    item: Item<K, T>,
    seqno: u64,
    virtual_finish_time: u64,
}

impl<K, T> HeapItem<K, T>
where
    T: AsRef<[u8]>,
{
    fn data_size(&self) -> usize {
        self.item.data().as_ref().len()
    }
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
            .item
            .flow_weight()
            .cmp(&other.0.item.flow_weight())
            .then_with(|| self.0.seqno.cmp(&other.0.seqno).reverse())
    }
}

#[derive(Debug, Clone)]
struct FlowInfo {
    last_virtual_finish_time: u64,
    size: usize,
    weight: NonZeroU8,
}

impl FlowInfo {
    fn inv_w(&self) -> u64 {
        SCALED_ONE / u64::from(self.weight.get())
    }
}
