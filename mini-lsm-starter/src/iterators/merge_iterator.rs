#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut merge = Self {
            iters: iters
                .into_iter()
                .enumerate()
                .filter_map(|(i, x)| {
                    if x.is_valid() {
                        Some(HeapWrapper(i, x))
                    } else {
                        None
                    }
                })
                .collect(),
            current: None,
        };
        merge.pop_into_current();
        merge
    }

    fn pop_into_current(&mut self) {
        self.current = self.iters.pop();
        // skip invalid
        while self.current.is_some() && !self.current.as_ref().unwrap().1.is_valid() {
            self.current = self.iters.pop();
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let Some(mut cur) = std::mem::take(&mut self.current) else {
            return Ok(());
        };

        self.pop_the_same_key_as(cur.1.key())?;
        cur.1.next()?;
        if cur.1.is_valid() {
            self.iters.push(cur);
        }

        self.pop_into_current();
        Ok(())
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> MergeIterator<I> {
    fn pop_the_same_key_as(&mut self, key: I::KeyType<'_>) -> Result<()> {
        while let Some(top) = self.iters.peek() {
            if !top.1.is_valid() {
                self.iters.pop();
                continue;
            }

            if top.1.key() == key {
                let mut top = self.iters.pop().unwrap();
                if let Err(e) = top.1.next() {
                    return Err(e);
                } else if top.1.is_valid() {
                    self.iters.push(top);
                }
            } else {
                break;
            }
        }

        Ok(())
    }
}
