#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        Ok(Self { inner: iter })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()
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

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
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
}
