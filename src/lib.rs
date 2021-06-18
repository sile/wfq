use std::collections::{BinaryHeap, HashMap};
use std::num::NonZeroUsize;

const SCALED_ONE: usize = 1 << 16;

#[derive(Debug, Clone)]
pub struct WeightedFairQueue<T> {
    items: BinaryHeap<Item<T>>,
    overflow: BinaryHeap<OverflowHeapItem<T>>,
    flows: HashMap<usize, FlowInfo>,
    max_queue_size: usize,
    max_flow_size: usize,
    vt: usize,
    wsum: usize,
    size: usize,
    ovfcnt: usize,
}

impl<T> WeightedFairQueue<T> {
    pub fn new(max_queue_size: usize, max_flow_size: usize) -> Self {
        assert!(max_flow_size <= max_queue_size);

        Self {
            items: BinaryHeap::new(),
            overflow: BinaryHeap::new(),
            flows: HashMap::new(),
            max_queue_size,
            max_flow_size,
            vt: 0,
            wsum: 0,
            size: 0,
            ovfcnt: 0,
        }
    }

    pub fn enqueue(&mut self, mut item: Item<T>) -> Option<Item<T>> {
        assert!(item.size.get() <= self.max_flow_size);

        if !self.flows.contains_key(&item.flow) {
            let fi = FlowInfo {
                last_vft: self.vt,
                weight: item.weight + 1,
                size: 0,
            };
            self.wsum += usize::from(fi.weight);
            self.flows.insert(item.flow, fi);
        }
        let fi = self.flows.get_mut(&item.flow).expect("unreachable");

        if fi.size + item.size.get() > self.max_flow_size {
            return Some(item);
        }

        item.vft = fi.last_vft + item.size.get() * fi.inv_w();
        fi.last_vft = item.vft;

        fi.size += item.size.get();

        if self.size + item.size.get() > self.max_queue_size {
            let ohi = OverflowHeapItem {
                item,
                arrord: self.ovfcnt,
            };
            self.ovfcnt += 1;
            self.overflow.push(ohi);
            None
        } else {
            self.size += item.size.get();
            self.items.push(item);
            None
        }
    }

    pub fn dequeue(&mut self) -> Option<Item<T>> {
        let item = if let Some(item) = self.items.pop() {
            item
        } else {
            return None;
        };

        self.vt += item.size.get() * self.inv_wsum();
        self.size -= item.size.get();

        let mut fi = self.flows.remove(&item.flow).expect("unreachable");
        fi.size -= item.size.get();
        if fi.size == 0 {
            self.flows.remove(&item.flow);
            self.wsum -= usize::from(fi.weight);
        } else {
            self.flows.insert(item.flow, fi);
        }

        while let Some(next) = self.overflow.pop() {
            if self.size + next.item.size.get() > self.max_queue_size {
                self.overflow.push(next);
                break;
            }

            self.size += next.item.size.get();
            self.items.push(next.item);
        }

        Some(item)
    }

    fn inv_wsum(&self) -> usize {
        SCALED_ONE / self.wsum
    }
}

#[derive(Debug, Clone)]
pub struct Item<T> {
    pub flow: usize,
    pub size: NonZeroUsize,
    pub weight: u8,
    pub value: T,
    pub vft: usize, // virtual finish time
}

impl<T> PartialEq for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        self.vft == other.vft
    }
}

impl<T> Eq for Item<T> {}

impl<T> PartialOrd for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vft.cmp(&other.vft).reverse()
    }
}

#[derive(Debug, Clone)]
pub struct OverflowHeapItem<T> {
    pub item: Item<T>,
    pub arrord: usize,
}

impl<T> PartialEq for OverflowHeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.item.weight == other.item.weight && self.arrord == other.arrord
    }
}

impl<T> Eq for OverflowHeapItem<T> {}

impl<T> PartialOrd for OverflowHeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for OverflowHeapItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.item
            .weight
            .cmp(&other.item.weight)
            .then_with(|| self.arrord.cmp(&other.arrord).reverse())
    }
}

#[derive(Debug, Clone)]
struct FlowInfo {
    last_vft: usize,
    size: usize,
    weight: u8,
}

impl FlowInfo {
    pub fn inv_w(&self) -> usize {
        SCALED_ONE / usize::from(self.weight)
    }
}
