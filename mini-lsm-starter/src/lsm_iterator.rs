#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    fmt::Debug,
    ops::{Bound, Deref},
};

use anyhow::{bail, Result};

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::KeyBytes,
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_when: Bound<Vec<u8>>,
    ended: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        Ok(Self {
            inner: iter,
            ended: false,
            end_when: Bound::Unbounded,
        })
    }

    pub(crate) fn new_with_range(iter: LsmIteratorInner, end_when: Bound<Vec<u8>>) -> Result<Self> {
        Ok(Self {
            ended: !iter.is_valid() || !in_bound(iter.key().raw_ref(), &end_when),
            inner: iter,
            end_when,
        })
    }
}

fn in_bound(key: &[u8], end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Included(e) => key <= e.deref(),
        Bound::Excluded(e) => key < e.deref(),
        Bound::Unbounded => true,
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.ended
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.ended = !self.inner.is_valid() || !in_bound(self.key(), &self.end_when);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        let mut this = Self {
            iter,
            has_errored: false,
        };
        while this.iter.is_valid() && this.iter.value().is_empty() {
            match this.iter.next() {
                Ok(_) => {}
                Err(e) => {
                    this.has_errored = true;
                    return this;
                }
            };
        }

        this
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I>
where
    for<'a> I::KeyType<'a>: Debug,
{
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.is_valid() {
            panic!("Invalid iterator");
        }

        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.is_valid() {
            panic!("Invalid iterator");
        }

        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Invalid iterator");
        }

        if !self.is_valid() {
            return Ok(());
        }

        loop {
            match self.iter.next() {
                Ok(_) => {}
                Err(e) => {
                    self.has_errored = true;
                    return Err(e);
                }
            };

            if !self.is_valid() {
                return Ok(());
            }

            if !self.value().is_empty() {
                return Ok(());
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
