#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::Ordering, sync::Arc};

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let current = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables
            .binary_search_by(|sst| {
                if sst.first_key().raw_ref() > key.raw_ref() {
                    Ordering::Greater
                } else if sst.last_key().raw_ref() < key.raw_ref() {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
            .unwrap_or_else(|e| e);
        if idx == sstables.len() {
            Ok(Self {
                current: None,
                next_sst_idx: idx,
                sstables,
            })
        } else {
            Ok(Self {
                current: Some(SsTableIterator::create_and_seek_to_key(
                    sstables[idx].clone(),
                    key,
                )?),
                next_sst_idx: idx,
                sstables,
            })
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(current) = self.current.as_mut() {
            current.next()?;
            if current.is_valid() {
                return Ok(());
            }
        }

        loop {
            if let Some(current) = self.current.as_mut() {
                if current.is_valid() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }

            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
