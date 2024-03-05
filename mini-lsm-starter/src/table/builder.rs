#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{borrow::BorrowMut, path::Path};

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.extract_built();
            assert!(self.builder.add(key, value));
        }
    }

    fn extract_built(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        if self.first_key.is_empty() {
            self.first_key = block.nth_key(0).to_vec();
        }
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_bytes(Bytes::copy_from_slice(block.nth_key(0))),
            last_key: Key::from_bytes(Bytes::copy_from_slice(
                block.nth_key(block.len_entries() - 1),
            )),
        });
        self.data.extend_from_slice(&block.encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.extract_built();
        let last_key = self.meta.last().unwrap().last_key.clone();
        let (data, meta_offset) = {
            let mut data = self.data;
            let meta_offset = data.len() as u32;
            BlockMeta::encode_block_meta(&self.meta, &mut data);
            data.extend(meta_offset.to_be_bytes());
            (data, meta_offset)
        };

        let f = FileObject::create(path.as_ref(), data)?;
        Ok(SsTable {
            file: f,
            block_meta: self.meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: Key::from_vec(self.first_key).into_key_bytes(),
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
