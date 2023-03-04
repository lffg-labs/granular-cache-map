use std::{
    collections::{hash_map::Entry, HashMap},
    hash::{BuildHasher, Hash},
    mem,
};

use crate::{Cache, CacheStrategy, WriteRef};

pub struct WriteBatch<'c, S, H>
where
    S: CacheStrategy,
{
    cache: &'c Cache<S, H>,
    entries: HashMap<S::Key, WriteRef<'c, S::Val>>,
}

impl<'c, S, H> WriteBatch<'c, S, H>
where
    S: CacheStrategy,
    S::Key: Hash,
    H: BuildHasher + Default,
{
    pub(crate) fn new(cache: &'c Cache<S, H>) -> WriteBatch<'c, S, H> {
        Self {
            cache,
            entries: HashMap::with_capacity(8),
        }
    }
}

impl<'c, S, H> WriteBatch<'c, S, H>
where
    S: CacheStrategy,
    S::Key: Hash + Eq + Copy,
    H: BuildHasher + Default,
{
    pub fn write<F, R>(&mut self, key: &S::Key, f: F) -> Result<R, S::Err>
    where
        F: for<'a> Fn(&'a S::Val) -> R,
    {
        match self.entries.entry(*key) {
            Entry::Occupied(entry) => {
                let val = entry.get();
                Ok(f(val))
            }
            Entry::Vacant(entry) => {
                let guard = self.cache.write(&key)?;
                let guard_ref = entry.insert(guard);
                Ok(f(guard_ref))
            }
        }
    }

    pub fn write_all<F, E>(mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(WriteRef<'c, S::Val>) -> Result<(), E>,
    {
        for entry in mem::take(&mut self.entries).into_values() {
            f(entry)?;
        }
        Ok(())
    }
}

impl<'c, S, H> Drop for WriteBatch<'c, S, H>
where
    S: CacheStrategy,
{
    fn drop(&mut self) {
        assert_eq!(self.entries.len(), 0);
    }
}
