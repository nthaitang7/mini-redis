use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Fixed per-entry accounting overhead in bytes.
pub const ENTRY_OVERHEAD_BYTES: usize = 96;

/// Estimate the accounted memory for one key and value.
pub fn estimate_entry_bytes(key: &[u8], value: &[u8]) -> usize {
    ENTRY_OVERHEAD_BYTES + key.len() + value.len()
}

/// Eviction behavior when the memory limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
}

impl EvictionPolicy {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "noeviction" => Some(Self::NoEviction),
            "allkeys-lru" => Some(Self::AllKeysLru),
            "volatile-lru" => Some(Self::VolatileLru),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
        }
    }
}

/// A store failure that maps to a Redis error reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    OutOfMemory,
    InvalidTtl,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => {
                formatter.write_str("OOM command not allowed when used memory > 'maxmemory'.")
            }
            Self::InvalidTtl => formatter.write_str("ERR invalid expire time"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type Clock = Box<dyn Fn() -> u64 + Send + Sync>;

/// Millisecond wall clock used by the server.
pub fn system_clock() -> Clock {
    Box::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    })
}

/// Process-local cache counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub expired_keys: u64,
    pub total_commands: u64,
}

#[derive(Debug, Clone)]
struct Entry {
    value: Vec<u8>,
    expires_at: Option<u64>,
    last_access: u64,
    version: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ExpiryRecord {
    at: u64,
    version: u64,
    key: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoveReason {
    Deleted,
    Replaced,
    Expired,
    Evicted,
}

/// In-memory string store with TTLs, approximate memory accounting, and eviction.
pub struct Store {
    entries: HashMap<Vec<u8>, Entry>,
    expiry: BinaryHeap<Reverse<ExpiryRecord>>,
    max_memory: usize,
    policy: EvictionPolicy,
    used_bytes: usize,
    next_stamp: u64,
    next_version: u64,
    clock: Clock,
    stats: StoreStats,
    started_at: Instant,
}

impl Store {
    pub fn new(max_memory: usize, policy: EvictionPolicy, clock: Clock) -> Self {
        Self {
            entries: HashMap::new(),
            expiry: BinaryHeap::new(),
            max_memory,
            policy,
            used_bytes: 0,
            next_stamp: 1,
            next_version: 1,
            clock,
            stats: StoreStats::default(),
            started_at: Instant::now(),
        }
    }

    pub fn used_memory(&self) -> usize {
        self.used_bytes
    }

    pub fn memory_limit(&self) -> usize {
        self.max_memory
    }

    pub fn policy(&self) -> EvictionPolicy {
        self.policy
    }

    pub fn stats(&self) -> &StoreStats {
        &self.stats
    }

    pub fn record_command(&mut self) {
        self.stats.total_commands = self.stats.total_commands.saturating_add(1);
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Return a value and update hit, miss, and LRU state.
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let now = self.now();
        self.expire_if_needed(key, now);
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);

        let value = self.entries.get_mut(key).map(|entry| {
            entry.last_access = stamp;
            entry.value.clone()
        });

        match value {
            Some(value) => {
                self.stats.hits = self.stats.hits.saturating_add(1);
                Some(value)
            }
            None => {
                self.stats.misses = self.stats.misses.saturating_add(1);
                None
            }
        }
    }

    /// Check for a live key and update hit, miss, and LRU state.
    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Write a value, optionally with a relative TTL, evicting when necessary. Returns true when the key did not previously exist.
    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> Result<bool, StoreError> {
        if ttl_ms == Some(0) {
            return Err(StoreError::InvalidTtl);
        }

        let now = self.now();
        self.sweep_at(now);
        let (created, old_size) = match self.entries.get(key) {
            Some(entry) => (false, estimate_entry_bytes(key, &entry.value)),
            None => (true, 0),
        };
        let new_size = estimate_entry_bytes(key, value);
        let expires_at = match ttl_ms {
            Some(ttl) => Some(now.checked_add(ttl).ok_or(StoreError::InvalidTtl)?),
            None => None,
        };

        if self.max_memory > 0 && new_size > self.max_memory {
            return Err(StoreError::OutOfMemory);
        }

        let projected = self
            .used_bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);

        if self.max_memory > 0 && projected > self.max_memory {
            if self.policy == EvictionPolicy::NoEviction {
                return Err(StoreError::OutOfMemory);
            }

            let mut candidates: Vec<(u64, Vec<u8>, usize)> = self
                .entries
                .iter()
                .filter(|(candidate, entry)| {
                    candidate.as_slice() != key
                        && !is_expired(entry, now)
                        && (self.policy == EvictionPolicy::AllKeysLru || entry.expires_at.is_some())
                })
                .map(|(candidate, entry)| {
                    (
                        entry.last_access,
                        candidate.clone(),
                        estimate_entry_bytes(candidate, &entry.value),
                    )
                })
                .collect();

            candidates.sort_by_key(|(stamp, _, _)| *stamp);

            let mut reclaimed = 0usize;
            let mut to_evict = Vec::new();

            for (_, candidate, size) in candidates {
                reclaimed = reclaimed.saturating_add(size);
                to_evict.push(candidate);

                if projected.saturating_sub(reclaimed) <= self.max_memory {
                    break;
                }
            }

            if projected.saturating_sub(reclaimed) > self.max_memory {
                return Err(StoreError::OutOfMemory);
            }

            for candidate in to_evict {
                self.remove_entry(&candidate, RemoveReason::Evicted);
            }
        }

        self.remove_entry(key, RemoveReason::Replaced);

        let version = self.next_version;
        self.next_version = self.next_version.wrapping_add(1);
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);

        self.entries.insert(
            key.to_vec(),
            Entry {
                value: value.to_vec(),
                expires_at,
                last_access: stamp,
                version,
            },
        );
        self.used_bytes = self.used_bytes.saturating_add(new_size);

        if let Some(expires_at) = expires_at {
            self.expiry.push(Reverse(ExpiryRecord {
                at: expires_at,
                version,
                key: key.to_vec(),
            }));
            self.compact_expiry_heap();
        }

        Ok(created)
    }

    /// Delete keys and return how many were live.
    pub fn delete(&mut self, keys: &[Vec<u8>]) -> i64 {
        let now = self.now();
        let mut deleted = 0;

        for key in keys {
            self.expire_if_needed(key, now);

            if self.remove_entry(key, RemoveReason::Deleted).is_some() {
                deleted += 1;
            }
        }

        deleted
    }

    /// Set or remove a TTL. A non-positive TTL deletes the key.
    pub fn expire(&mut self, key: &[u8], ttl_ms: i64) -> bool {
        let now = self.now();
        self.expire_if_needed(key, now);

        if !self.entries.contains_key(key) {
            return false;
        }

        if ttl_ms <= 0 {
            self.remove_entry(key, RemoveReason::Expired);
            return true;
        }

        let expires_at = now.saturating_add(ttl_ms as u64);
        let version = self.next_version;
        self.next_version = self.next_version.wrapping_add(1);

        if let Some(entry) = self.entries.get_mut(key) {
            entry.expires_at = Some(expires_at);
            entry.version = version;
        }

        self.expiry.push(Reverse(ExpiryRecord {
            at: expires_at,
            version,
            key: key.to_vec(),
        }));
        self.compact_expiry_heap();
        true
    }

    /// Remove a TTL from a live key.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        let now = self.now();
        self.expire_if_needed(key, now);

        let has_ttl = self
            .entries
            .get(key)
            .map(|entry| entry.expires_at.is_some())
            .unwrap_or(false);

        if !has_ttl {
            return false;
        }

        let version = self.next_version;
        self.next_version = self.next_version.wrapping_add(1);

        if let Some(entry) = self.entries.get_mut(key) {
            entry.expires_at = None;
            entry.version = version;
        }

        true
    }

    /// Remaining TTL in milliseconds, or -1 without expiry and -2 when missing.
    pub fn ttl_milliseconds(&mut self, key: &[u8]) -> i64 {
        let now = self.now();
        self.expire_if_needed(key, now);

        match self.entries.get(key) {
            None => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(expires_at) => expires_at.saturating_sub(now) as i64,
            },
        }
    }

    /// Remaining TTL rounded up to whole seconds, with the same -1 and -2 sentinels.
    pub fn ttl_seconds(&mut self, key: &[u8]) -> i64 {
        let milliseconds = self.ttl_milliseconds(key);

        if milliseconds < 0 {
            return milliseconds;
        }

        if milliseconds % 1000 == 0 {
            milliseconds / 1000
        } else {
            milliseconds / 1000 + 1
        }
    }

    /// Number of live keys after sweeping due expirations.
    pub fn key_count(&mut self) -> usize {
        self.sweep();
        self.entries.len()
    }

    /// Number of live keys carrying a TTL.
    pub fn expiring_key_count(&mut self) -> usize {
        self.sweep();
        self.entries
            .values()
            .filter(|entry| entry.expires_at.is_some())
            .count()
    }

    /// Average remaining TTL in milliseconds across keys that have one.
    pub fn average_ttl_milliseconds(&mut self) -> u64 {
        self.sweep();
        let now = self.now();
        let mut total = 0u64;
        let mut count = 0u64;

        for entry in self.entries.values() {
            if let Some(expires_at) = entry.expires_at {
                if expires_at > now {
                    total = total.saturating_add(expires_at - now);
                    count += 1;
                }
            }
        }

        if count == 0 {
            0
        } else {
            total / count
        }
    }

    /// Remove expired keys whose heap records are due, returning the count.
    pub fn sweep(&mut self) -> usize {
        let now = self.now();
        self.sweep_at(now)
    }

    fn sweep_at(&mut self, now: u64) -> usize {
        let mut expired = 0;

        loop {
            let due = match self.expiry.peek() {
                Some(Reverse(record)) => record.at <= now,
                None => false,
            };

            if !due {
                break;
            }

            let record = match self.expiry.pop() {
                Some(Reverse(record)) => record,
                None => break,
            };

            let should_remove = self
                .entries
                .get(&record.key)
                .map(|entry| {
                    entry.version == record.version
                        && entry
                            .expires_at
                            .map_or(false, |expires_at| expires_at <= now)
                })
                .unwrap_or(false);

            if should_remove
                && self
                    .remove_entry(&record.key, RemoveReason::Expired)
                    .is_some()
            {
                expired += 1;
            }
        }

        expired
    }

    /// Clear all keys and memory counts. Statistics are retained.
    pub fn flush(&mut self) {
        self.entries.clear();
        self.expiry.clear();
        self.used_bytes = 0;
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    fn expire_if_needed(&mut self, key: &[u8], now: u64) -> bool {
        let expired = self
            .entries
            .get(key)
            .map(|entry| is_expired(entry, now))
            .unwrap_or(false);

        if expired {
            self.remove_entry(key, RemoveReason::Expired);
            true
        } else {
            false
        }
    }

    fn remove_entry(&mut self, key: &[u8], reason: RemoveReason) -> Option<usize> {
        let entry = self.entries.remove(key)?;
        let size = estimate_entry_bytes(key, &entry.value);
        self.used_bytes = self.used_bytes.saturating_sub(size);

        match reason {
            RemoveReason::Expired => {
                self.stats.expired_keys = self.stats.expired_keys.saturating_add(1);
            }
            RemoveReason::Evicted => {
                self.stats.evictions = self.stats.evictions.saturating_add(1);
            }
            RemoveReason::Deleted | RemoveReason::Replaced => {}
        }

        Some(size)
    }

    fn compact_expiry_heap(&mut self) {
        let limit = self.entries.len().saturating_mul(4).saturating_add(1024);

        if self.expiry.len() <= limit {
            return;
        }

        let records: Vec<ExpiryRecord> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                entry.expires_at.map(|expires_at| ExpiryRecord {
                    at: expires_at,
                    version: entry.version,
                    key: key.clone(),
                })
            })
            .collect();

        self.expiry.clear();

        for record in records {
            self.expiry.push(Reverse(record));
        }
    }
}

fn is_expired(entry: &Entry, now: u64) -> bool {
    entry
        .expires_at
        .map_or(false, |expires_at| expires_at <= now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    struct TestStore {
        store: Store,
        now: Arc<AtomicU64>,
    }

    impl TestStore {
        fn new(max_memory: usize, policy: EvictionPolicy) -> Self {
            let now = Arc::new(AtomicU64::new(0));
            let clock_now = Arc::clone(&now);
            let store = Store::new(
                max_memory,
                policy,
                Box::new(move || clock_now.load(Ordering::Relaxed)),
            );
            Self { store, now }
        }

        fn advance(&self, milliseconds: u64) {
            self.now.fetch_add(milliseconds, Ordering::Relaxed);
        }
    }

    fn value() -> &'static [u8] {
        b"x"
    }

    #[test]
    fn sets_gets_and_deletes() {
        let mut test = TestStore::new(0, EvictionPolicy::AllKeysLru);
        assert!(test.store.set(b"key", b"value", None).unwrap());
        assert_eq!(test.store.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(test.store.delete(&[b"key".to_vec()]), 1);
        assert_eq!(test.store.get(b"key"), None);
    }

    #[test]
    fn replaces_values_and_clears_ttl() {
        let mut test = TestStore::new(0, EvictionPolicy::AllKeysLru);
        test.store.set(b"key", b"first", Some(1000)).unwrap();
        assert_eq!(test.store.ttl_milliseconds(b"key"), 1000);
        test.store.set(b"key", b"second", None).unwrap();
        test.advance(5000);
        assert_eq!(test.store.get(b"key"), Some(b"second".to_vec()));
        assert_eq!(test.store.ttl_milliseconds(b"key"), -1);
    }

    #[test]
    fn expires_lazily_and_actively() {
        let mut test = TestStore::new(0, EvictionPolicy::AllKeysLru);
        test.store.set(b"key", value(), Some(500)).unwrap();
        assert_eq!(test.store.ttl_seconds(b"key"), 1);
        test.advance(499);
        assert_eq!(test.store.ttl_milliseconds(b"key"), 1);
        test.advance(1);
        assert_eq!(test.store.get(b"key"), None);
        assert_eq!(test.store.stats().expired_keys, 1);

        test.store.set(b"other", value(), Some(100)).unwrap();
        test.advance(100);
        assert_eq!(test.store.sweep(), 1);
        assert_eq!(test.store.key_count(), 0);
    }

    #[test]
    fn ignores_stale_expiry_records() {
        let mut test = TestStore::new(0, EvictionPolicy::AllKeysLru);
        test.store.set(b"key", b"first", Some(100)).unwrap();
        test.store.set(b"key", b"second", None).unwrap();
        test.advance(100);
        assert_eq!(test.store.sweep(), 0);
        assert_eq!(test.store.get(b"key"), Some(b"second".to_vec()));
    }

    #[test]
    fn expires_and_persists() {
        let mut test = TestStore::new(0, EvictionPolicy::AllKeysLru);
        test.store.set(b"key", value(), None).unwrap();
        assert!(test.store.expire(b"key", 1000));
        assert_eq!(test.store.ttl_milliseconds(b"key"), 1000);
        test.advance(250);
        assert_eq!(test.store.ttl_milliseconds(b"key"), 750);
        assert!(test.store.persist(b"key"));
        assert_eq!(test.store.ttl_milliseconds(b"key"), -1);
        assert!(!test.store.persist(b"key"));
        assert!(test.store.expire(b"key", 0));
        assert_eq!(test.store.get(b"key"), None);
    }

    #[test]
    fn tracks_memory() {
        let mut test = TestStore::new(0, EvictionPolicy::AllKeysLru);
        let size = estimate_entry_bytes(b"key", b"value");
        test.store.set(b"key", b"value", None).unwrap();
        assert_eq!(test.store.used_memory(), size);
        test.store.set(b"key", b"another", None).unwrap();
        assert_eq!(
            test.store.used_memory(),
            estimate_entry_bytes(b"key", b"another")
        );
        test.store.delete(&[b"key".to_vec()]);
        assert_eq!(test.store.used_memory(), 0);
    }

    #[test]
    fn rejects_oom_and_preserves_state() {
        let item_size = estimate_entry_bytes(b"a", value());
        let mut test = TestStore::new(item_size, EvictionPolicy::NoEviction);
        test.store.set(b"a", value(), None).unwrap();
        assert_eq!(
            test.store.set(b"b", value(), None),
            Err(StoreError::OutOfMemory)
        );
        assert_eq!(test.store.get(b"a"), Some(value().to_vec()));
        assert_eq!(test.store.get(b"b"), None);
    }

    #[test]
    fn evicts_least_recently_used() {
        let item_size = estimate_entry_bytes(b"a", value());
        let mut test = TestStore::new(item_size * 3, EvictionPolicy::AllKeysLru);
        test.store.set(b"a", value(), None).unwrap();
        test.store.set(b"b", value(), None).unwrap();
        test.store.set(b"c", value(), None).unwrap();
        test.store.get(b"a");
        test.store.set(b"d", value(), None).unwrap();

        assert!(test.store.get(b"a").is_some());
        assert!(test.store.get(b"b").is_none());
        assert!(test.store.get(b"c").is_some());
        assert!(test.store.get(b"d").is_some());
        assert_eq!(test.store.stats().evictions, 1);
    }

    #[test]
    fn volatile_lru_only_evicts_keys_with_ttl() {
        let item_size = estimate_entry_bytes(b"a", value());
        let mut test = TestStore::new(item_size * 3, EvictionPolicy::VolatileLru);
        test.store.set(b"a", value(), None).unwrap();
        test.store.set(b"b", value(), Some(1000)).unwrap();
        test.store.set(b"c", value(), Some(1000)).unwrap();
        test.store.get(b"b");
        test.store.set(b"d", value(), None).unwrap();

        assert!(test.store.get(b"a").is_some());
        assert!(test.store.get(b"b").is_some());
        assert!(test.store.get(b"c").is_none());
        assert!(test.store.get(b"d").is_some());
    }

    #[test]
    fn volatile_lru_fails_without_candidates() {
        let item_size = estimate_entry_bytes(b"a", value());
        let mut test = TestStore::new(item_size * 2, EvictionPolicy::VolatileLru);
        test.store.set(b"a", value(), None).unwrap();
        test.store.set(b"b", value(), None).unwrap();
        assert_eq!(
            test.store.set(b"c", value(), None),
            Err(StoreError::OutOfMemory)
        );
    }
}
