#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

use crate::key::{Key, KeySlice};

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

    pub fn len_entries(&self) -> usize {
        self.offsets.len()
    }

    pub fn nth_key(&self, n: usize) -> (&[u8], &[u8]) {
        let mut start = self.offsets[n] as usize;
        self.decode_key_at_offset(&mut start)
    }

    pub(crate) fn decode_key_at_offset(&self, offset: &mut usize) -> (&[u8], &[u8]) {
        if *offset == 0 {
            let ans: (&[u8], &[u8]) = (self.first_key(), &[]);
            *offset += 2 + ans.0.len();
            ans
        } else {
            let d = &self.data;
            let overlap_len = u16::from_be_bytes([d[*offset], d[*offset + 1]]) as usize;
            let rest_len = u16::from_be_bytes([d[*offset + 2], d[*offset + 3]]) as usize;
            let overlapped = &self.first_key()[..overlap_len];
            let rest = &d[*offset + 4..*offset + 4 + rest_len];
            *offset += 4 + rest_len;
            (overlapped, rest)
        }
    }

    pub(crate) fn first_key(&self) -> &[u8] {
        let start = self.offsets[0] as usize;
        let d = &self.data;
        let len = u16::from_be_bytes([d[start], d[start + 1]]) as usize;
        &d[start + 2..start + 2 + len]
    }
}
