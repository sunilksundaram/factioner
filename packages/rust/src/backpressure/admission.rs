use crate::backpressure::priority_queue::{PriorityQueue, QueuedItem};
use crate::types::{AdmissionPolicy, BackpressureConfig, JobPriority};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionCode {
    QueueFull,
    Saturated,
}

pub struct AdmissionDecision {
    pub accepted: bool,
    pub reason: Option<String>,
    pub code: Option<AdmissionCode>,
    pub dropped: bool,
}

pub struct AdmissionController {
    config: BackpressureConfig,
    slot_notifier: Arc<Notify>,
    lock: Mutex<()>,
}

impl AdmissionController {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            slot_notifier: Arc::new(Notify::new()),
            lock: Mutex::new(()),
        }
    }

    /// Atomically evaluate admission and enqueue on accept.
    /// Returns `(decision, dropped_item, rejected_item)`.
    /// `rejected_item` is `Some` when the incoming job was not enqueued.
    /// Notify is NOT fired for DropOldest+enqueue (net depth unchanged).
    pub async fn admit_and_enqueue<T>(
        &self,
        queue: &Mutex<PriorityQueue<T>>,
        item: QueuedItem<T>,
    ) -> (AdmissionDecision, Option<QueuedItem<T>>, Option<QueuedItem<T>>) {
        let priority = item.priority;
        let capacity = self.config.max_queue_size;

        {
            let _guard = self.lock.lock().await;
            let mut q = queue.lock().await;
            let current = q.len();

            if current < capacity {
                let saturation = (current as f64 / capacity as f64) * 100.0;
                if saturation >= self.config.shedding_threshold_percentage
                    && priority == JobPriority::Batch
                {
                    return (
                        AdmissionDecision {
                            accepted: false,
                            reason: Some(format!(
                                "Queue is {:.0}% saturated; batch jobs shed proactively",
                                saturation
                            )),
                            code: Some(AdmissionCode::Saturated),
                            dropped: false,
                        },
                        None,
                        Some(item),
                    );
                }
                q.enqueue(item, false);
                return (
                    AdmissionDecision {
                        accepted: true,
                        reason: None,
                        code: None,
                        dropped: false,
                    },
                    None,
                    None,
                );
            }

            match self.config.admission_policy {
                AdmissionPolicy::Reject | AdmissionPolicy::DropNewest => {
                    return (
                        AdmissionDecision {
                            accepted: false,
                            reason: Some(format!(
                                "Queue capacity reached ({} jobs). Backpressure applied.",
                                capacity
                            )),
                            code: Some(AdmissionCode::QueueFull),
                            dropped: false,
                        },
                        None,
                        Some(item),
                    );
                }
                AdmissionPolicy::DropOldest => {
                    if (priority as usize) < (JobPriority::Batch as usize) {
                        if let Some(dropped) = q.drop_oldest_low_priority() {
                            q.enqueue(item, false);
                            return (
                                AdmissionDecision {
                                    accepted: true,
                                    reason: None,
                                    code: None,
                                    dropped: true,
                                },
                                Some(dropped),
                                None,
                            );
                        }
                    }
                    return (
                        AdmissionDecision {
                            accepted: false,
                            reason: Some(
                                "Queue full of critical/high priority work.".to_string(),
                            ),
                            code: Some(AdmissionCode::QueueFull),
                            dropped: false,
                        },
                        None,
                        Some(item),
                    );
                }
                AdmissionPolicy::Timeout => {
                    // Fall through to wait loop below.
                }
            }
        }

        let wait_ms = self.config.admission_wait_timeout_ms;
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
        let mut pending = Some(item);

        loop {
            if tokio::time::Instant::now() >= deadline {
                return (
                    AdmissionDecision {
                        accepted: false,
                        reason: Some(format!(
                            "Queue capacity reached. Timed out waiting {}ms for slot.",
                            wait_ms
                        )),
                        code: Some(AdmissionCode::QueueFull),
                        dropped: false,
                    },
                    None,
                    pending.take(),
                );
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::select! {
                _ = self.slot_notifier.notified() => {}
                _ = tokio::time::sleep(remaining.min(std::time::Duration::from_millis(500))) => {}
            }

            let _g = self.lock.lock().await;
            let mut q = queue.lock().await;
            if q.len() < capacity {
                if let Some(item) = pending.take() {
                    q.enqueue(item, false);
                }
                return (
                    AdmissionDecision {
                        accepted: true,
                        reason: None,
                        code: None,
                        dropped: false,
                    },
                    None,
                    None,
                );
            }
        }
    }

    /// Notify waiters only when net queue depth decreased.
    pub fn notify_slot_available(&self) {
        self.slot_notifier.notify_waiters();
    }

    pub fn get_config(&self) -> &BackpressureConfig {
        &self.config
    }
}
