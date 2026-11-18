use crate::commands::error::CommandError;
use crate::protocol::AccountId;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

/// Scope key used for cooldown and max-concurrency buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BucketType {
    #[default]
    Default,
    User,
}

impl BucketType {
    pub fn key_for(&self, author_id: &AccountId) -> Option<AccountId> {
        match self {
            Self::Default => None,
            Self::User => Some(author_id.clone()),
        }
    }
}

/// Sliding token-bucket rate limiter attached to a command or group.
#[derive(Debug, Clone)]
pub struct Cooldown {
    pub rate: usize,
    pub per: Duration,
    pub bucket: BucketType,
    window_start: Option<Instant>,
    tokens: usize,
}

impl Cooldown {
    pub fn new(rate: usize, per: Duration, bucket: BucketType) -> Self {
        let clamped_rate = rate.max(1);
        Self {
            rate: clamped_rate,
            per,
            bucket,
            window_start: None,
            tokens: clamped_rate,
        }
    }

    pub fn get_tokens(&self, now: Instant) -> usize {
        match self.window_start {
            Some(start) if now.saturating_duration_since(start) > self.per => self.rate,
            _ => self.tokens,
        }
    }

    pub fn update_rate_limit(&mut self, now: Instant) -> Option<Duration> {
        self.tokens = self.get_tokens(now);

        if self.tokens == self.rate {
            self.window_start = Some(now);
        }

        if self.tokens == 0 {
            let elapsed = self
                .window_start
                .map(|start| now.saturating_duration_since(start))
                .unwrap_or_default();
            return Some(self.per.saturating_sub(elapsed));
        }

        self.tokens -= 1;
        if self.tokens == 0 {
            self.window_start = Some(now);
        }
        None
    }

    pub fn reset(&mut self) {
        self.tokens = self.rate;
        self.window_start = None;
    }
}

/// Per-bucket cooldown state container.
#[derive(Debug)]
pub struct CooldownMapping {
    original: Cooldown,
    buckets: DashMap<Option<AccountId>, Arc<Mutex<Cooldown>>>,
}

impl CooldownMapping {
    pub fn new(rate: usize, per: Duration, bucket: BucketType) -> Arc<Self> {
        Arc::new(Self {
            original: Cooldown::new(rate, per, bucket),
            buckets: DashMap::new(),
        })
    }

    pub async fn update_rate_limit(&self, author_id: &AccountId) -> Option<Duration> {
        let key = self.original.bucket.key_for(author_id);
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(self.original.clone())))
            .value()
            .clone();
        let mut cooldown = bucket.lock().await;
        cooldown.update_rate_limit(Instant::now())
    }

    pub fn reset_all(&self) {
        self.buckets.clear();
    }

    pub fn bucket(&self) -> BucketType {
        self.original.bucket
    }
}

/// Concurrent command execution limiter.
#[derive(Debug)]
pub struct MaxConcurrency {
    pub number: usize,
    pub per: BucketType,
    pub wait: bool,
    semaphores: DashMap<Option<AccountId>, Arc<Semaphore>>,
}

impl MaxConcurrency {
    pub fn new(number: usize, per: BucketType, wait: bool) -> Arc<Self> {
        Arc::new(Self {
            number: number.max(1),
            per,
            wait,
            semaphores: DashMap::new(),
        })
    }

    pub async fn acquire(&self, author_id: &AccountId) -> Result<OwnedSemaphorePermit, CommandError> {
        let key = self.per.key_for(author_id);
        let sem = self
            .semaphores
            .entry(key)
            .or_insert_with(|| Arc::new(Semaphore::new(self.number)))
            .value()
            .clone();

        if self.wait {
            sem.acquire_owned()
                .await
                .map_err(|_| CommandError::MaxConcurrencyReached {
                    number: self.number,
                    per: self.per,
                })
        } else {
            sem.try_acquire_owned()
                .map_err(|_| CommandError::MaxConcurrencyReached {
                    number: self.number,
                    per: self.per,
                })
        }
    }
}
