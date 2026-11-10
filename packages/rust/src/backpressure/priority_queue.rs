use crate::types::JobPriority;
use std::collections::{BTreeMap, VecDeque};

pub struct QueuedItem<T> {
    pub id: String,
    pub priority: JobPriority,
    pub payload: T,
    pub tenant_id: Option<String>,
    pub created_at: std::time::Instant,
    pub retries_remaining: usize,
    pub attempts: usize,
    pub previous_decorrelated_delay: Option<u64>,
    pub estimated_budget: crate::types::BudgetDeduction,
    pub avoid_endpoint_ids: Vec<String>,
    pub tenant_charged: bool,
    pub queue_timeout_ms: u64,
    pub execution_timeout_ms: u64,
}

pub struct PriorityQueue<T> {
    tiers: BTreeMap<JobPriority, BTreeMap<String, VecDeque<QueuedItem<T>>>>,
    pointers: BTreeMap<JobPriority, usize>,
    total_count: usize,
}

impl<T> PriorityQueue<T> {
    pub fn new() -> Self {
        let mut tiers = BTreeMap::new();
        let mut pointers = BTreeMap::new();
        for prio in [
            JobPriority::Critical,
            JobPriority::High,
            JobPriority::Normal,
            JobPriority::Batch,
        ] {
            tiers.insert(prio, BTreeMap::new());
            pointers.insert(prio, 0);
        }
        Self {
            tiers,
            pointers,
            total_count: 0,
        }
    }

    pub fn enqueue(&mut self, item: QueuedItem<T>, front: bool) {
        let tenant_key = item
            .tenant_id
            .clone()
            .unwrap_or_else(|| "_default".to_string());
        let tier_map = self.tiers.get_mut(&item.priority).unwrap();
        let queue = tier_map.entry(tenant_key).or_insert_with(VecDeque::new);
        if front {
            queue.push_front(item);
        } else {
            queue.push_back(item);
        }
        self.total_count += 1;
    }

    pub fn dequeue(&mut self) -> Option<QueuedItem<T>> {
        if self.total_count == 0 {
            return None;
        }

        for prio in [
            JobPriority::Critical,
            JobPriority::High,
            JobPriority::Normal,
            JobPriority::Batch,
        ] {
            let tier_map = self.tiers.get_mut(&prio).unwrap();
            let active_keys: Vec<String> = tier_map
                .iter()
                .filter(|(_, q)| !q.is_empty())
                .map(|(k, _)| k.clone())
                .collect();

            if !active_keys.is_empty() {
                let ptr = self.pointers.entry(prio).or_insert(0);
                let chosen_key = active_keys[*ptr % active_keys.len()].clone();
                *ptr += 1;

                if let Some(queue) = tier_map.get_mut(&chosen_key) {
                    if let Some(item) = queue.pop_front() {
                        self.total_count -= 1;
                        return Some(item);
                    }
                }
            }
        }
        None
    }

    /// Never drops Critical.
    pub fn drop_oldest_low_priority(&mut self) -> Option<QueuedItem<T>> {
        if self.total_count == 0 {
            return None;
        }

        for prio in [JobPriority::Batch, JobPriority::Normal, JobPriority::High] {
            let tier_map = self.tiers.get_mut(&prio).unwrap();
            let mut largest_key = None;
            let mut max_len = 0usize;

            for (k, q) in tier_map.iter() {
                if q.len() > max_len {
                    max_len = q.len();
                    largest_key = Some(k.clone());
                }
            }

            if let Some(key) = largest_key {
                if let Some(queue) = tier_map.get_mut(&key) {
                    if let Some(item) = queue.pop_front() {
                        self.total_count -= 1;
                        return Some(item);
                    }
                }
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.total_count
    }

    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Drain all queued items (used on pool drain timeout).
    pub fn drain_all(&mut self) -> Vec<QueuedItem<T>> {
        let mut out = Vec::with_capacity(self.total_count);
        while let Some(item) = self.dequeue() {
            out.push(item);
        }
        out
    }
}

impl<T> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}
