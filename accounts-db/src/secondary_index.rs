use {
    dashmap::{mapref::entry::Entry::Occupied, DashMap},
    log::*,
    solana_sdk::{pubkey::Pubkey, timing::AtomicInterval},
    std::{
        collections::HashSet,
        fmt::Debug,
        sync::{
            atomic::{AtomicU64, Ordering},
            RwLock,
        },
        time::{SystemTime, UNIX_EPOCH},
    },
};

// The only cases where an inner key should map to a different outer key is
// if the key had different account data for the indexed key across different
// slots. As this is rare, it should be ok to use a Vec here over a HashSet, even
// though we are running some key existence checks.
pub type SecondaryReverseIndexEntry = RwLock<Vec<Pubkey>>;

pub trait SecondaryIndexEntry: Debug {
    fn insert_if_not_exists(&self, key: &Pubkey, inner_keys_count: &AtomicU64);
    // Removes a value from the set. Returns whether the value was present in the set.
    fn remove_inner_key(&self, key: &Pubkey) -> bool;
    fn is_empty(&self) -> bool;
    fn keys(&self) -> Vec<Pubkey>;
    fn len(&self) -> usize;
}

#[derive(Debug, Default)]
pub struct SecondaryIndexStats {
    last_report: AtomicInterval,
    num_inner_keys: AtomicU64,
}

#[derive(Debug, Default)]
pub struct DashMapSecondaryIndexEntry {
    account_keys: DashMap<Pubkey, ()>,
}

pub static READ: StatRecorder = StatRecorder::new();
pub static INSERT: StatRecorder = StatRecorder::new();
pub static REMOVE: StatRecorder = StatRecorder::new();
pub static LEN: StatRecorder = StatRecorder::new();

pub fn print_stats_loop(interval: u64) {
    print_stats_loop_json(interval);
}

pub fn print_stats_loop_csv(interval: u64) {
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    loop {
        println!(
            "{} - secondary index stats:",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - start
        );
        println!("  operation,{}", StatSnapshot::csv_header());
        println!("  READ,{}", READ.get_snapshot().csv());
        println!("  INSERT,{}", INSERT.get_snapshot().csv());
        println!("  REMOVE,{}", REMOVE.get_snapshot().csv());
        println!("  LEN,{}", LEN.get_snapshot().csv());
        std::thread::sleep(std::time::Duration::from_secs(interval));
    }
}

pub fn print_stats_loop_json(interval: u64) {
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    loop {
        println!(
            "{} - secondary index stats:",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - start
        );
        // println!("  operation,{}", StatSnapshot::csv_header());
        let summary = StatSummary {
            timestamp: (now_nanos() / 1_000_000_000) as u64,
            read: READ.get_snapshot(),
            insert: INSERT.get_snapshot(),
            remove: REMOVE.get_snapshot(),
            len: LEN.get_snapshot(),
        };
        println!("{},", serde_json::to_string(&summary).unwrap());
        std::thread::sleep(std::time::Duration::from_secs(interval));
    }
}

pub struct StatRecorder {
    read_lock: std::sync::Mutex<()>,
    count_and_shard: AtomicU64,
    shards: [StatShard; 2],
}

struct StatShard {
    sum: AtomicU64,
    count: AtomicU64,
    max: AtomicU64,
    min: AtomicU64,
}

impl StatShard {
    pub const fn new() -> Self {
        Self {
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            max: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
        }
    }

    pub fn update_max(&self, x: u64) {
        _ = self
            .max
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (x > old).then_some(x)
            });
    }

    pub fn update_min(&self, x: u64) {
        _ = self
            .min
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (x < old).then_some(x)
            });
    }
}

impl StatRecorder {
    pub const fn new() -> Self {
        Self {
            read_lock: std::sync::Mutex::new(()),
            count_and_shard: AtomicU64::new(0),
            shards: [StatShard::new(), StatShard::new()],
        }
    }

    pub fn get_snapshot(&self) -> StatSnapshot {
        let lock = self.read_lock.lock();
        let cas = self.count_and_shard.fetch_xor(1, Ordering::Acquire);
        let shard = &self.shards[(cas % 2) as usize];
        let count = cas >> 1;

        while shard
            .count
            .compare_exchange_weak(count, 0, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {}

        let sum = shard.sum.load(Ordering::Relaxed);
        let max = shard.max.load(Ordering::Relaxed);
        let min = shard.min.load(Ordering::Relaxed);

        let other_shard = &self.shards[((cas + 1) % 2) as usize];
        other_shard.count.fetch_add(count, Ordering::Relaxed);
        other_shard.sum.fetch_add(sum, Ordering::Relaxed);
        other_shard.update_max(max);
        other_shard.update_min(min);

        shard.count.store(0, Ordering::Relaxed);
        shard.sum.store(0, Ordering::Relaxed);
        shard.min.store(u64::MAX, Ordering::Relaxed);
        shard.max.store(0, Ordering::Relaxed);

        drop(lock);

        StatSnapshot {
            count,
            sum,
            max,
            min,
        }
    }

    pub fn record(&self, x: u64) {
        let cas = self.count_and_shard.fetch_add(2, Ordering::Acquire);
        let shard = &self.shards[(cas % 2) as usize];
        shard.sum.fetch_add(x, Ordering::Relaxed);
        shard.update_max(x);
        shard.update_min(x);
        shard.count.fetch_add(1, Ordering::Release);
    }
}

#[derive(Serialize)]
pub struct StatSummary {
    timestamp: u64,
    read: StatSnapshot,
    insert: StatSnapshot,
    remove: StatSnapshot,
    len: StatSnapshot,
}

#[derive(Serialize)]
pub struct StatSnapshot {
    pub sum: u64,
    pub count: u64,
    pub min: u64,
    pub max: u64,
}

impl StatSnapshot {
    pub fn mean(&self) -> Option<u64> {
        self.sum.checked_div(self.count)
    }

    pub fn json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn csv_header() -> String {
        format!("mean,min,max,count")
    }

    pub fn csv(&self) -> String {
        let string = |opt: Option<u64>| {
            if let Some(x) = opt {
                x.to_string()
            } else {
                "".to_owned()
            }
        };
        format!(
            "{},{},{},{}",
            string(self.mean()),
            string((self.count != 0).then_some(self.min)),
            string((self.count != 0).then_some(self.max)),
            self.count
        )
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn record_duration(recorder: &StatRecorder, start_nanos: u128) {
    recorder.record((now_nanos() - start_nanos).try_into().unwrap());
}

impl SecondaryIndexEntry for DashMapSecondaryIndexEntry {
    fn insert_if_not_exists(&self, key: &Pubkey, inner_keys_count: &AtomicU64) {
        let start = now_nanos();
        if self.account_keys.get(key).is_none() {
            self.account_keys.entry(*key).or_insert_with(|| {
                inner_keys_count.fetch_add(1, Ordering::Relaxed);
            });
        }
        record_duration(&INSERT, start);
    }

    fn remove_inner_key(&self, key: &Pubkey) -> bool {
        let start = now_nanos();
        let ret = self.account_keys.remove(key).is_some();
        record_duration(&REMOVE, start);
        ret
    }

    fn is_empty(&self) -> bool {
        let start = now_nanos();
        let ret = self.account_keys.is_empty();
        record_duration(&LEN, start);
        ret
    }

    fn keys(&self) -> Vec<Pubkey> {
        let start = now_nanos();
        let ret = self
            .account_keys
            .iter()
            .map(|entry_ref| *entry_ref.key())
            .collect();
        record_duration(&READ, start);
        ret
    }

    fn len(&self) -> usize {
        let start = now_nanos();
        let ret = self.account_keys.len();
        record_duration(&LEN, start);
        ret
    }
}

#[derive(Debug, Default)]
pub struct RwLockSecondaryIndexEntry {
    account_keys: RwLock<HashSet<Pubkey>>,
}

impl SecondaryIndexEntry for RwLockSecondaryIndexEntry {
    fn insert_if_not_exists(&self, key: &Pubkey, inner_keys_count: &AtomicU64) {
        let start = now_nanos();
        let exists = self.account_keys.read().unwrap().contains(key);
        if !exists {
            let mut w_account_keys = self.account_keys.write().unwrap();
            w_account_keys.insert(*key);
            inner_keys_count.fetch_add(1, Ordering::Relaxed);
        };
        INSERT.record((now_nanos() - start).try_into().unwrap());
    }

    fn remove_inner_key(&self, key: &Pubkey) -> bool {
        let start = now_nanos();
        let ret = self.account_keys.write().unwrap().remove(key);
        REMOVE.record((now_nanos() - start).try_into().unwrap());
        ret
    }

    fn is_empty(&self) -> bool {
        let start = now_nanos();
        let ret = self.account_keys.read().unwrap().is_empty();
        LEN.record((now_nanos() - start).try_into().unwrap());
        ret
    }

    fn keys(&self) -> Vec<Pubkey> {
        let start = now_nanos();
        let ret = self.account_keys.read().unwrap().iter().cloned().collect();
        READ.record((now_nanos() - start).try_into().unwrap());
        ret
    }

    fn len(&self) -> usize {
        let start = now_nanos();
        let ret = self.account_keys.read().unwrap().len();
        LEN.record((now_nanos() - start).try_into().unwrap());
        ret
    }
}

#[derive(Debug, Default)]
pub struct SecondaryIndex<SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send> {
    metrics_name: &'static str,
    // Map from index keys to index values
    pub index: DashMap<Pubkey, SecondaryIndexEntryType>,
    pub reverse_index: DashMap<Pubkey, SecondaryReverseIndexEntry>,
    stats: SecondaryIndexStats,
}

impl<SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send>
    SecondaryIndex<SecondaryIndexEntryType>
{
    pub fn new(metrics_name: &'static str) -> Self {
        Self {
            metrics_name,
            ..Self::default()
        }
    }

    pub fn insert(&self, key: &Pubkey, inner_key: &Pubkey) {
        {
            let pubkeys_map = self
                .index
                .get(key)
                .unwrap_or_else(|| self.index.entry(*key).or_default().downgrade());

            pubkeys_map.insert_if_not_exists(inner_key, &self.stats.num_inner_keys);
        }

        {
            let outer_keys = self.reverse_index.get(inner_key).unwrap_or_else(|| {
                self.reverse_index
                    .entry(*inner_key)
                    .or_insert(RwLock::new(Vec::with_capacity(1)))
                    .downgrade()
            });

            let should_insert = !outer_keys.read().unwrap().contains(key);
            if should_insert {
                let mut w_outer_keys = outer_keys.write().unwrap();
                if !w_outer_keys.contains(key) {
                    w_outer_keys.push(*key);
                }
            }
        }

        if self.stats.last_report.should_update(1000) {
            datapoint_info!(
                self.metrics_name,
                ("num_secondary_keys", self.index.len() as i64, i64),
                (
                    "num_inner_keys",
                    self.stats.num_inner_keys.load(Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "num_reverse_index_keys",
                    self.reverse_index.len() as i64,
                    i64
                ),
            );
        }
    }

    // Only safe to call from `remove_by_inner_key()` due to asserts
    fn remove_index_entries(&self, outer_key: &Pubkey, removed_inner_key: &Pubkey) {
        let is_outer_key_empty = {
            let inner_key_map = self
                .index
                .get_mut(outer_key)
                .expect("If we're removing a key, then it must have an entry in the map");
            // If we deleted a pubkey from the reverse_index, then the corresponding entry
            // better exist in this index as well or the two indexes are out of sync!
            assert!(inner_key_map.value().remove_inner_key(removed_inner_key));
            inner_key_map.is_empty()
        };

        // Delete the `key` if the set of inner keys is empty
        if is_outer_key_empty {
            // Other threads may have interleaved writes to this `key`,
            // so double-check again for its emptiness
            if let Occupied(key_entry) = self.index.entry(*outer_key) {
                if key_entry.get().is_empty() {
                    key_entry.remove();
                }
            }
        }
    }

    pub fn remove_by_inner_key(&self, inner_key: &Pubkey) {
        // Save off which keys in `self.index` had slots removed so we can remove them
        // after we purge the reverse index
        let mut removed_outer_keys: HashSet<Pubkey> = HashSet::new();

        // Check if the entry for `inner_key` in the reverse index is empty
        // and can be removed
        if let Some((_, outer_keys_set)) = self.reverse_index.remove(inner_key) {
            for removed_outer_key in outer_keys_set.into_inner().unwrap().into_iter() {
                removed_outer_keys.insert(removed_outer_key);
            }
        }

        // Remove this value from those keys
        for outer_key in &removed_outer_keys {
            self.remove_index_entries(outer_key, inner_key);
        }

        // Safe to `fetch_sub()` here because a dead key cannot be removed more than once,
        // and the `num_inner_keys` must have been incremented by exactly removed_outer_keys.len()
        // in previous unique insertions of `inner_key` into `self.index` for each key
        // in `removed_outer_keys`
        self.stats
            .num_inner_keys
            .fetch_sub(removed_outer_keys.len() as u64, Ordering::Relaxed);
    }

    pub fn get(&self, key: &Pubkey) -> Vec<Pubkey> {
        if let Some(inner_keys_map) = self.index.get(key) {
            inner_keys_map.keys()
        } else {
            vec![]
        }
    }

    /// log top 20 (owner, # accounts) in descending order of # accounts
    pub fn log_contents(&self) {
        let mut entries = self
            .index
            .iter()
            .map(|entry| (entry.value().len(), *entry.key()))
            .collect::<Vec<_>>();
        entries.sort_unstable();
        entries
            .iter()
            .rev()
            .take(20)
            .for_each(|(v, k)| info!("owner: {}, accounts: {}", k, v));
    }
}
