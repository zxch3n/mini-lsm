#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Default::default(),
            data: Default::default(),
            block_size,
            first_key: Key::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// If it returns false, the block is not modified.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.block_size + key.len() + value.len() >= 65536 {
            panic!("Key and value are too large");
        }

        let new_len = 2 + 2 + key.len() + value.len();
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        } else if new_len + self.bytes_size() > self.block_size {
            // too large
            return false;
        }

        self.data.reserve(new_len);
        let start_offset = self.data.len() as u16;
        let bytes = (key.len() as u16).to_be_bytes();
        self.data.push(bytes[0]);
        self.data.push(bytes[1]);
        self.data.extend_from_slice(key.raw_ref());

        let end_offset = self.data.len() as u16;
        let bytes = (value.len() as u16).to_be_bytes();
        self.data.push(bytes[0]);
        self.data.push(bytes[1]);
        self.data.extend_from_slice(value);

        self.offsets.push(start_offset);
        true
    }

    fn bytes_size(&mut self) -> usize {
        self.data.len() + self.offsets.len() * 2 + 2
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
