#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut ans = Self::new(block);
        ans.seek_to_first();
        ans
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut ans = Self::create_and_seek_to_first(block);
        ans.seek_to_key(key);
        ans
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.block.data[self.value_range.0..self.value_range.1].as_ref()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        // we should allow eq
        self.value_range.0 <= self.value_range.1
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_nth(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let d = &self.block.data;
        let offset = self.value_range.1;
        if offset == self.block.data.len() {
            self.key.clear();
            self.value_range = (self.block.data.len(), 0);
            return;
        }

        let key_len = u16::from_be_bytes([d[offset], d[offset + 1]]) as usize;
        self.key = Key::from_vec(d[offset + 2..offset + 2 + key_len].to_vec());
        let value_len =
            u16::from_be_bytes([d[offset + 2 + key_len], d[offset + 3 + key_len]]) as usize;
        let value_start = offset + 4 + key_len;
        self.value_range = (value_start, value_start + value_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut min = 0;
        let mut max = self.block.offsets.len() - 1;
        let mut mid = (min + max) / 2;
        let mut mid_v = self.get_nth_key(mid);
        while min != max {
            match mid_v.cmp(&key) {
                std::cmp::Ordering::Less => {
                    min = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    max = mid;
                }
                std::cmp::Ordering::Equal => {
                    break;
                }
            }

            mid = (min + max) / 2;
            mid_v = self.get_nth_key(mid);
        }

        // find the first key that >= key
        while mid > 0 && self.get_nth_key(mid - 1) >= key {
            mid -= 1;
        }

        self.seek_nth(mid)
    }

    fn get_nth_key(&mut self, n: usize) -> KeySlice {
        let start = self.block.offsets[n] as usize;
        let d = &self.block.data;
        let len = u16::from_be_bytes([d[start], d[start + 1]]) as usize;
        Key::from_slice(&d[start + 2..start + 2 + len])
    }

    fn seek_nth(&mut self, n: usize) {
        let start = self.block.offsets[n] as usize;
        let d = &self.block.data;
        let key_len = u16::from_be_bytes([d[start], d[start + 1]]) as usize;
        self.key = Key::from_vec(d[start + 2..start + 2 + key_len].to_vec());
        let value_len =
            u16::from_be_bytes([d[start + 2 + key_len], d[start + 3 + key_len]]) as usize;
        self.value_range = (start + 4 + key_len, start + 4 + key_len + value_len);
    }
}
