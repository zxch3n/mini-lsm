#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut ans = Self { a, b };
        ans.skip_b_that_eq_a()?;
        Ok(ans)
    }

    fn skip_b_that_eq_a(&mut self) -> Result<(), anyhow::Error> {
        while self.a.is_valid() && self.b.is_valid() && self.b.key() == self.a.key() {
            self.b.next()?;
        }

        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    fn is_cur_a(&self) -> bool {
        if self.a.is_valid() && self.b.is_valid() {
            self.a.key() <= self.b.key()
        } else {
            self.a.is_valid()
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.is_cur_a() {
            true => self.a.key(),
            false => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.is_cur_a() {
            true => self.a.value(),
            false => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_cur_a() {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        self.skip_b_that_eq_a()
    }
}
