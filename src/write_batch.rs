use std::{
    collections::{hash_map::Entry, HashMap},
    hash::{BuildHasher, Hash},
    mem,
};

use crate::{Cache, CacheStrategy, WriteRef};

/// A write batch represents a collection of write cache entries are grouped to
/// be flushed together.
///
/// The `flush_all` method must be called before the `WriteBatch` instance is
/// dropped. Otherwise, a panic will be raised when dropping it.
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
{
    /// Constructs a new  `WriteBatch`.
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
    /// Creates a scope on which the value corresponding to the given key may be
    /// modified.
    pub fn write<F, R>(&mut self, key: &S::Key, f: F) -> Result<R, S::Err>
    where
        F: for<'a> Fn(&'a mut S::Val) -> R,
    {
        match self.entries.entry(*key) {
            Entry::Occupied(mut entry) => {
                let val = entry.get_mut();
                Ok(f(val))
            }
            Entry::Vacant(entry) => {
                let guard = self.cache.write(&key)?;
                let guard_ref = entry.insert(guard);
                Ok(f(guard_ref))
            }
        }
    }

    /// Flushes all the modifications using the given function, which may fail.
    ///
    /// Callers must ensure previous writes are reverted in case of any
    /// posterior errors in the batch sequence.
    pub fn flush_all<F, E>(mut self, mut f: F) -> Result<(), E>
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
        if self.entries.len() != 0 {
            panic!("dropped `WriteBatch` without calling `flush_all`")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        test_utils::{TestHashBuilder, TestStrategy},
        Cache,
    };

    #[test]
    fn test_grouped_flush() {
        let s = TestStrategy::default();
        let c = Cache::<TestStrategy, TestHashBuilder>::new::<4>(s);

        assert_eq!(c.clone_strategy().count(), 0);

        {
            let data = c.read(&1).unwrap();
            assert_eq!(&*data, "1one");
            assert_eq!(c.clone_strategy().count(), 1);
        }

        let mut wb = c.write_batch();

        wb.write(&1, |val| val.push_str("-mod")).unwrap();
        assert_eq!(c.clone_strategy().count(), 1);

        wb.write(&2, |val| val.push_str("-mod")).unwrap();
        assert_eq!(c.clone_strategy().count(), 2);

        wb.write(&1, |val| val.push_str("-mod")).unwrap();
        assert_eq!(c.clone_strategy().count(), 2);

        let mut hs = HashSet::from(["1one-mod-mod", "2two-mod"]);
        wb.flush_all(|val| {
            assert!(hs.remove(val.as_str()));
            Ok::<_, ()>(())
        })
        .unwrap();
        assert!(hs.is_empty());

        {
            let data = c.read(&1).unwrap();
            assert_eq!(&*data, "1one-mod-mod");
            assert_eq!(c.clone_strategy().count(), 2);
        }
    }
}
