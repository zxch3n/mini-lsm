use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        if amplification_percent(snapshot) >= self.options.max_size_amplification_percent as f64 {
            // full
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        {
            // triggered by size ratio
            let mut acc_size = 0;
            for (i, (_level, vec)) in snapshot.levels.iter().enumerate() {
                let this_size = vec.len();
                if i + 1 > self.options.min_merge_width
                    && acc_size as f64 / this_size as f64
                        >= (100.0 + self.options.size_ratio as f64) / 100.0
                {
                    let mut tiers = Vec::new();
                    for (_, level) in &snapshot.levels[..=i] {
                        tiers.push((level[0], level.clone()));
                    }
                    return Some(TieredCompactionTask {
                        tiers,
                        bottom_tier_included: i == snapshot.levels.len() - 1,
                    });
                }

                acc_size += this_size;
            }
        }

        let to_compact = snapshot.levels.len() - self.options.num_tiers + 2;
        let mut tiers = Vec::new();
        for (_, level) in &snapshot.levels[..to_compact] {
            tiers.push((level[0], level.clone()));
        }
        Some(TieredCompactionTask {
            bottom_tier_included: to_compact == snapshot.levels.len(),
            tiers,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let set: HashSet<usize> = task.tiers.iter().map(|x| x.0).collect();
        let mut new_snapshot = snapshot.clone();
        let mut i = 0;
        let mut insert_pos = 0;
        let mut del = Vec::new();
        new_snapshot.levels.retain(|x| {
            if set.contains(&x.0) {
                del.extend(&x.1);
                insert_pos = i;
                false
            } else {
                i += 1;
                true
            }
        });

        new_snapshot
            .levels
            .insert(insert_pos, (output[0], output.to_vec()));
        (new_snapshot, del)
    }
}

fn amplification_percent(snapshot: &LsmStorageState) -> f64 {
    let last_level_size = snapshot.levels.last().unwrap().1.len();
    let engine_size = snapshot
        .levels
        .iter()
        .map(|(_, level)| level.len())
        .sum::<usize>()
        - last_level_size;
    engine_size as f64 * 100.0 / last_level_size as f64
}
