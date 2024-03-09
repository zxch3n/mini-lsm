#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let block_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(SsTableIterator {
            table,
            blk_iter: block_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let idx = table.find_block_idx(key);
        let block = table.read_block_cached(idx)?;
        let block_iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(SsTableIterator {
            table,
            blk_iter: block_iter,
            blk_idx: idx,
        })
    }

    /// Create a new iterator and seek to the first key-value pair which > `key`.
    pub fn create_and_seek_to_key_exclusive(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let idx = table.find_block_idx(key);
        let block = table.read_block_cached(idx)?;
        let block_iter = BlockIterator::create_and_seek_to_key(block, key);
        let mut iter = SsTableIterator {
            table,
            blk_iter: block_iter,
            blk_idx: idx,
        };
        if iter.is_valid() && iter.key() == key {
            iter.next()?;
        }

        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let idx = self.table.find_block_idx(key);
        if idx == self.blk_idx {
            self.blk_iter.seek_to_key(key);
            Ok(())
        } else {
            let block = self.table.read_block_cached(idx)?;
            self.blk_idx = idx;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
            Ok(())
        }
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        while !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx >= self.table.block_meta.len() {
                return Ok(());
            }

            let block = self.table.read_block(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }

        Ok(())
    }
}
