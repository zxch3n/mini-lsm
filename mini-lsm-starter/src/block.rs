#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut ans = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        ans.extend_from_slice(&self.data);
        for offset in self.offsets.iter() {
            ans.extend_from_slice(&offset.to_be_bytes());
        }
        ans.extend_from_slice(&(self.offsets.len() as u16).to_be_bytes());
        ans.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = u16::from_be_bytes([data[data.len() - 2], data[data.len() - 1]]) as usize;
        let end_of_data = data.len() - 2 - len * 2;
        let block_data = data[..end_of_data].to_vec();
        let mut offsets = Vec::with_capacity(len);
        for i in (end_of_data..data.len() - 2).step_by(2) {
            offsets.push(u16::from_be_bytes([data[i], data[i + 1]]));
        }

        Self {
            data: block_data,
            offsets,
        }
    }
}
