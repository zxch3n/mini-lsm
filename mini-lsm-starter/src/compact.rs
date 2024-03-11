#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn build_from_iter<I: StorageIterator>(
        &self,
        mut iter: I,
        skip_empty: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        for<'a> I::KeyType<'a>: Into<KeySlice<'a>>,
    {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut ans = Vec::new();
        while iter.is_valid() {
            if skip_empty && iter.value().is_empty() {
                iter.next()?;
                continue;
            }

            builder.add(iter.key().into(), iter.value());
            if builder.estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                let sst =
                    builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                builder = SsTableBuilder::new(self.options.block_size);
                ans.push(Arc::new(sst));
            }

            iter.next()?;
        }

        if !builder.is_empty() {
            let id = self.next_sst_id();
            let sst = builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
            ans.push(Arc::new(sst));
        }

        Ok(ans)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = {
            let s = self.state.read();
            Arc::clone(&s)
        };

        let ans = match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(task) => {
                if task.upper_level.is_some() {
                    let iter: TwoMergeIterator<SstConcatIterator, SstConcatIterator> =
                        TwoMergeIterator::create(
                            SstConcatIterator::create_and_seek_to_first(
                                task.upper_level_sst_ids
                                    .iter()
                                    .map(|x| state.sstables[x].clone())
                                    .collect(),
                            )?,
                            SstConcatIterator::create_and_seek_to_first(
                                task.lower_level_sst_ids
                                    .iter()
                                    .map(|x| state.sstables[x].clone())
                                    .collect(),
                            )?,
                        )?;
                    self.build_from_iter(iter, task.is_lower_level_bottom_level)?
                } else {
                    // If upper level is from l0, we need to use merge iterator
                    let iter: TwoMergeIterator<MergeIterator<SsTableIterator>, SstConcatIterator> =
                        TwoMergeIterator::create(
                            MergeIterator::create(
                                task.upper_level_sst_ids
                                    .iter()
                                    .map(|x| -> anyhow::Result<Box<SsTableIterator>> {
                                        let table = state.sstables[x].clone();
                                        Ok(Box::new(SsTableIterator::create_and_seek_to_first(
                                            table,
                                        )?))
                                    })
                                    .collect::<Result<Vec<Box<_>>>>()?,
                            ),
                            SstConcatIterator::create_and_seek_to_first(
                                task.lower_level_sst_ids
                                    .iter()
                                    .map(|x| state.sstables[x].clone())
                                    .collect(),
                            )?,
                        )?;
                    self.build_from_iter(iter, false)?
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let iter: TwoMergeIterator<MergeIterator<SsTableIterator>, SstConcatIterator> =
                    TwoMergeIterator::create(
                        MergeIterator::create(
                            l0_sstables
                                .iter()
                                .map(|x| -> anyhow::Result<Box<SsTableIterator>> {
                                    let table = state.sstables[x].clone();
                                    Ok(Box::new(SsTableIterator::create_and_seek_to_first(table)?))
                                })
                                .collect::<Result<Vec<Box<_>>>>()?,
                        ),
                        SstConcatIterator::create_and_seek_to_first(
                            l1_sstables
                                .iter()
                                .map(|x| state.sstables[x].clone())
                                .collect(),
                        )?,
                    )?;

                self.build_from_iter(iter, true)?
            }
        };

        Ok(ans)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let tables = {
            let s = self.state.read();
            (s.l0_sstables.clone(), s.levels[0].1.clone())
        };

        let original_l0: HashSet<usize> = tables.0.iter().cloned().collect();
        let original_l1_len = tables.1.len();
        let ans = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: tables.0,
            l1_sstables: tables.1,
        })?;
        {
            let _g = self.state_lock.lock();
            let mut binding = self.state.write();
            let state = Arc::make_mut(&mut binding);
            state.l0_sstables.retain(|x| {
                if original_l0.contains(x) {
                    let _sst = state.sstables.remove(x).unwrap();
                    // TODO: remove sst file
                    false
                } else {
                    true
                }
            });
            assert_eq!(state.levels[0].1.len(), original_l1_len);
            for s in state.levels[0].1.drain(..) {
                let _sst = state.sstables.remove(&s).unwrap();
                // TODO: remove sst file
            }
            for sstable in ans {
                state.levels[0].1.push(sstable.sst_id());
                state.sstables.insert(sstable.sst_id(), sstable);
            }
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let _g = self.state_lock.lock();
        let snapshot = {
            let s = self.state.read();
            Arc::clone(&s)
        };
        let Some(compaction) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };
        let new_ssts = self.compact(&compaction)?;
        let mut state = self.state.write();
        let state_mut = Arc::make_mut(&mut state);
        let mut output = Vec::with_capacity(new_ssts.len());
        for sst in new_ssts {
            let id = sst.sst_id();
            state_mut.sstables.insert(id, sst);
            output.push(id);
        }

        let (new_statet, del) =
            self.compaction_controller
                .apply_compaction_result(state_mut, &compaction, &output);
        *state_mut = new_statet;
        for i in del {
            state_mut.sstables.remove(&i);
            // TODO: remove sst file?
        }
        drop(state);
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let s = self.state.read();
        if s.imm_memtables.len() < self.options.num_memtable_limit {
            return Ok(());
        }

        drop(s);
        self.force_flush_next_imm_memtable()
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}

fn collect_iter<I: StorageIterator>(mut iter: I) -> HashMap<Vec<u8>, Vec<u8>>
where
    for<'a> I::KeyType<'a>: Into<KeySlice<'a>>,
{
    let mut ans = HashMap::new();
    while iter.is_valid() {
        ans.insert(iter.key().into().raw_ref().to_vec(), iter.value().to_vec());
        iter.next().unwrap();
    }
    ans
}
