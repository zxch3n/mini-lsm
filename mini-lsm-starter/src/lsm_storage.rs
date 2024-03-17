#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        let _a = self.inner.state_lock.lock();
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        // create file and directory on path if not exist
        let _ = std::fs::create_dir_all(path);
        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = {
            let s = self.state.read();
            Arc::clone(&s)
        };

        if let Some(ans) = state.memtable.get(key) {
            if ans.is_empty() {
                return Ok(None);
            }

            return Ok(Some(ans));
        }

        for table in state.imm_memtables.iter() {
            if let Some(ans) = table.get(key) {
                if ans.is_empty() {
                    return Ok(None);
                }

                return Ok(Some(ans));
            }
        }

        if self.compaction_controller.flush_to_l0() {
            for id in state.l0_sstables.iter() {
                let t = state.sstables.get(id).unwrap();
                if !t.may_contain(key) {
                    continue;
                }

                let iter =
                    SsTableIterator::create_and_seek_to_key(t.clone(), KeySlice::from_slice(key))?;
                if iter.is_valid() && iter.key().raw_ref() == key {
                    if iter.value().is_empty() {
                        return Ok(None);
                    }

                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                }
            }
        }

        for (_i, level) in state.levels.iter() {
            let result = level.binary_search_by(|x| {
                let t = state.sstables.get(x).unwrap();
                if t.first_key().raw_ref() > key {
                    Ordering::Greater
                } else if t.last_key().raw_ref() < key {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            });

            match result {
                Ok(x) => {
                    let t = state.sstables.get(&level[x]).unwrap();
                    if !t.may_contain(key) {
                        continue;
                    }

                    let iter = SsTableIterator::create_and_seek_to_key(
                        t.clone(),
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        if iter.value().is_empty() {
                            return Ok(None);
                        }

                        return Ok(Some(Bytes::copy_from_slice(iter.value())));
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let state = self.state.read();
        state.memtable.put(key, value)?;
        if state.memtable.approximate_size() >= self.options.target_sst_size {
            drop(state);
            let lock = self.state_lock.lock();
            // Check again. The value may change after acquiring the lock.
            let read = self.state.read();
            if read.memtable.approximate_size() >= self.options.target_sst_size {
                drop(read);
                self.force_freeze_memtable(&lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let table = MemTable::create(self.next_sst_id());
        let mut state = self.state.write();
        // FIXME: whether this is correct?
        let state = Arc::make_mut(&mut *state);
        let target: &mut Arc<MemTable> = &mut state.memtable;
        let old = std::mem::replace(target, Arc::new(table));
        state.imm_memtables.insert(0, old);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let table = {
            let mut s = self.state.write();
            let s = Arc::make_mut(&mut s);
            let Some(table) = s.imm_memtables.pop() else {
                return Ok(());
            };
            table
        };
        let _s = self.state_lock.lock();
        let id = self.next_sst_id();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        table.flush(&mut builder)?;
        let sstable = builder.build(
            id,
            Some(self.block_cache.clone()),
            self.path.join(format!("{}.sst", id)).clone(),
        )?;
        let mut s = self.state.write();
        let s = Arc::make_mut(&mut s);
        if self.compaction_controller.flush_to_l0() {
            s.l0_sstables.insert(0, id);
        } else {
            s.levels.insert(0, (id, vec![id]));
        }

        s.sstables.insert(id, Arc::new(sstable));
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let g = self.state.read();
            Arc::clone(&g)
        };
        let mut memtable_iters = Vec::with_capacity(state.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(state.memtable.scan(lower, upper)));
        for t in state.imm_memtables.iter() {
            memtable_iters.push(Box::new(t.scan(lower, upper)));
        }

        let mut l0_sstable_iters = Vec::with_capacity(state.sstables.len());
        if self.compaction_controller.flush_to_l0() {
            for id in state.l0_sstables.iter() {
                let t = state.sstables.get(id).unwrap();
                if !range_overlap(
                    (lower, upper),
                    (t.first_key().raw_ref(), t.last_key().raw_ref()),
                ) {
                    continue;
                }

                l0_sstable_iters.push(match lower {
                    Bound::Included(x) => Box::new(
                        SsTableIterator::create_and_seek_to_key(t.clone(), KeySlice::from_slice(x))
                            .unwrap(),
                    ),
                    Bound::Excluded(x) => Box::new(
                        SsTableIterator::create_and_seek_to_key_exclusive(
                            t.clone(),
                            KeySlice::from_slice(x),
                        )
                        .unwrap(),
                    ),
                    Bound::Unbounded => {
                        Box::new(SsTableIterator::create_and_seek_to_first(t.clone()).unwrap())
                    }
                });
            }
        }

        let mut l1_and_above = Vec::new();
        for (_level, ids) in state.levels.iter() {
            let mut sstables = Vec::new();
            for id in ids {
                let t = state.sstables.get(id).unwrap();
                if !range_overlap(
                    (lower, upper),
                    (t.first_key().raw_ref(), t.last_key().raw_ref()),
                ) {
                    continue;
                }

                sstables.push(t.clone());
            }

            let concat_iter = match lower {
                Bound::Included(x) => {
                    SstConcatIterator::create_and_seek_to_key(sstables, Key::from_slice(x))?
                }
                Bound::Excluded(x) => {
                    let mut iter =
                        SstConcatIterator::create_and_seek_to_key(sstables, Key::from_slice(x))?;
                    if iter.is_valid() && iter.key().raw_ref() == x {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables)?,
            };

            l1_and_above.push(Box::new(concat_iter));
        }

        let iter = FusedIterator::new(LsmIterator::new_with_range(
            TwoMergeIterator::create(
                TwoMergeIterator::create(
                    MergeIterator::create(memtable_iters),
                    MergeIterator::create(l0_sstable_iters),
                )
                .unwrap(),
                MergeIterator::create(l1_and_above),
            )
            .unwrap(),
            match upper {
                Bound::Included(e) => Bound::Included(e.to_vec()),
                Bound::Excluded(e) => Bound::Excluded(e.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        )?);
        Ok(iter)
    }

    pub(crate) fn log_all(&self) {
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        while iter.is_valid() {
            dbg!(iter.key(), iter.value());
            iter.next().unwrap();
        }
    }

    pub(crate) fn get_all(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut ans = HashMap::new();
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        while iter.is_valid() {
            ans.insert(iter.key().to_vec(), iter.value().to_vec());
            iter.next().unwrap();
        }

        ans
    }
}

fn range_overlap(a: (Bound<&[u8]>, Bound<&[u8]>), b: (&[u8], &[u8])) -> bool {
    if matches!(a, (Bound::Unbounded, Bound::Unbounded)) {
        return true;
    }

    let (a_lower, a_upper) = a;
    let (b_lower, b_upper) = b;
    match a_lower {
        Bound::Included(x) => {
            if x > b_upper {
                return false;
            }
        }
        Bound::Excluded(x) => {
            if x >= b_upper {
                return false;
            }
        }
        Bound::Unbounded => {}
    }

    match a_upper {
        Bound::Included(x) => {
            if x < b_lower {
                return false;
            }
        }
        Bound::Excluded(x) => {
            if x <= b_lower {
                return false;
            }
        }
        Bound::Unbounded => {}
    }

    true
}
