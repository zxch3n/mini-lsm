#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering;
use std::fs::File;
use std::io::Cursor;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for block in block_meta {
            buf.extend((block.offset as u32).to_be_bytes());
            buf.extend((block.first_key.len() as u16).to_be_bytes());
            buf.extend((block.last_key.len() as u16).to_be_bytes());
            buf.extend_from_slice(block.first_key.raw_ref());
            buf.extend_from_slice(block.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut ans = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32();
            let first_key_len = buf.get_u16();
            let last_key_len = buf.get_u16();
            let first_key = buf.copy_to_bytes(first_key_len as usize);
            let last_key = buf.copy_to_bytes(last_key_len as usize);
            ans.push(BlockMeta {
                offset: offset as usize,
                first_key: Key::from_bytes(first_key),
                last_key: Key::from_bytes(last_key),
            })
        }

        ans
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let f = file.0.as_ref().unwrap();
        let bloom_offset = read_last_u32(f, &file, file.1 as usize)?;
        let bloom_section = file.read(bloom_offset as u64, file.1 - 4 - bloom_offset as u64)?;
        let bloom = Bloom::decode(&bloom_section)?;
        let meta_offset = read_last_u32(f, &file, bloom_offset as usize)?;
        let mut block_meta_section = vec![0; bloom_offset - meta_offset - 4];
        f.read_exact_at(&mut block_meta_section, meta_offset as u64)?;
        let block_meta = BlockMeta::decode_block_meta(Cursor::new(block_meta_section));
        Ok(Self {
            file,
            first_key: block_meta[0].first_key.clone(),
            last_key: block_meta[block_meta.len() - 1].last_key.clone(),
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_offset = self.block_meta[block_idx].offset;
        let end_offset = if block_idx < self.block_meta.len() - 1 {
            self.block_meta[block_idx + 1].offset
        } else {
            self.block_meta_offset
        };
        let content = self
            .file
            .read(start_offset as u64, end_offset as u64 - start_offset as u64)?;
        Ok(Arc::new(Block::decode(&content)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let Some(cache) = self.block_cache.as_ref() else {
            return self.read_block(block_idx);
        };

        let key = (self.id, block_idx);
        if let Some(ans) = cache.get(&key) {
            Ok(ans)
        } else {
            let block = self.read_block(block_idx)?;
            cache.insert(key, block.clone());
            Ok(block)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .binary_search_by(|block| {
                if block.first_key.raw_ref() > key.raw_ref() {
                    Ordering::Greater
                } else if block.last_key.raw_ref() < key.raw_ref() {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
            .unwrap_or_else(|x| if x == self.block_meta.len() { x - 1 } else { x })
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if key < self.first_key().raw_ref() || key > self.last_key().raw_ref() {
            return false;
        }

        if let Some(bloom) = self.bloom.as_ref() {
            return bloom.may_contain(farmhash::fingerprint32(key));
        }

        true
    }
}

fn read_last_u32(f: &File, file: &FileObject, end: usize) -> Result<usize, anyhow::Error> {
    let mut offset_bytes = [0, 0, 0, 0];
    f.read_exact_at(&mut offset_bytes, end as u64 - 4)?;
    let offset = u32::from_be_bytes(offset_bytes) as usize;
    Ok(offset)
}
