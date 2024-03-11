use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot
                    .levels
                    .first()
                    .map(|x| x.1.clone())
                    .unwrap_or_default(),
                is_lower_level_bottom_level: false,
            });
        }

        for level in 1..snapshot.levels.len() {
            let this_level = &snapshot.levels[level - 1];
            let next_level = &snapshot.levels[level];
            if this_level.1.is_empty() {
                continue;
            }

            if next_level.1.len() * 100 < self.options.size_ratio_percent * this_level.1.len() {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: this_level.1.clone(),
                    lower_level: level + 1,
                    lower_level_sst_ids: next_level.1.clone(),
                    is_lower_level_bottom_level: level == self.options.max_levels - 1,
                });
            }
        }

        if snapshot.levels.len() < self.options.max_levels
            && !snapshot.levels.last().unwrap().1.is_empty()
        {
            let level = snapshot.levels.len();
            return Some(SimpleLeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: snapshot.levels.last().unwrap().1.clone(),
                lower_level: level + 1,
                lower_level_sst_ids: Vec::new(),
                is_lower_level_bottom_level: level == self.options.max_levels - 1,
            });
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut del = task.upper_level_sst_ids.clone();
        del.extend(task.lower_level_sst_ids.iter().cloned());
        if task.upper_level.is_none() {
            assert_eq!(task.lower_level, 1);
            let set: HashSet<_> = task.upper_level_sst_ids.iter().cloned().collect();
            let l0: Vec<_> = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !set.contains(x))
                .cloned()
                .collect();

            let mut levels = snapshot.levels.clone();
            debug_assert_eq!(&levels[0].1, &task.lower_level_sst_ids);
            levels[0].1 = output.to_vec();
            (
                LsmStorageState {
                    memtable: snapshot.memtable.clone(),
                    imm_memtables: snapshot.imm_memtables.clone(),
                    l0_sstables: l0,
                    levels,
                    sstables: snapshot.sstables.clone(),
                },
                del,
            )
        } else {
            let mut levels = snapshot.levels.clone();
            debug_assert_eq!(&levels[task.lower_level - 1].1, &task.lower_level_sst_ids);
            debug_assert_eq!(
                &levels[task.upper_level.unwrap() - 1].1,
                &task.upper_level_sst_ids
            );
            while task.lower_level > levels.len() {
                let level = levels.len() + 1;
                levels.push((level, Vec::new()));
            }

            levels[task.lower_level - 1].1 = output.to_vec();
            levels[task.upper_level.unwrap() - 1].1.clear();
            (
                LsmStorageState {
                    memtable: snapshot.memtable.clone(),
                    imm_memtables: snapshot.imm_memtables.clone(),
                    l0_sstables: snapshot.l0_sstables.clone(),
                    levels,
                    sstables: snapshot.sstables.clone(),
                },
                del,
            )
        }
    }
}
