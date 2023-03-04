use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use tracing::info;

/// A cache strategy implementations. Provides information about the cache's key
/// and value types. It also provides a mechanism to load new values to the
/// cache.
pub trait CacheStrategy {
    type Key;
    type Val;
    type Err;

    /// Loads the value for the given key.
    fn load(&mut self, key: &Self::Key) -> Result<Self::Val, Self::Err>;

    /// Checks if the given key corresponds to the given value. If not (i.e,
    /// `false` returned), one assumes a cache key conflict.
    fn match_kv(key: &Self::Key, val: &Self::Val) -> bool;
}

/// The cache over a given [`CacheStrategy`].
pub struct Cache<S, H = RandomState>
where
    S: CacheStrategy,
    H: BuildHasher + Default,
{
    entries: Box<[RwLock<Option<S::Val>>]>,
    strategy: Mutex<S>,
    hasher: H,
}

impl<S, H> Cache<S, H>
where
    S: CacheStrategy,
    S::Key: Hash,
    H: BuildHasher + Default,
{
    const EL: RwLock<Option<S::Val>> = RwLock::new(None);

    /// Constructs a new cache.
    pub fn new<const CAPACITY: usize>(strategy: S) -> Cache<S, H> {
        Cache {
            entries: Vec::from([Self::EL; CAPACITY]).into_boxed_slice(),
            strategy: Mutex::new(strategy),
            hasher: H::default(),
        }
    }

    /// Computes the index using the given key.
    fn key(&self, key: &S::Key) -> &RwLock<Option<S::Val>> {
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let i = h.finish() as usize % self.entries.len();
        unsafe { self.entries.get_unchecked(i) }
    }

    /// Acquires the value by the given key, for read.
    pub fn read(&self, key: &S::Key) -> Result<ReadRef<'_, S::Val>, S::Err> {
        info!("acquiring read lock...");
        let mut guard = self.key(key).read().unwrap();

        if guard.is_none() || S::match_kv(key, guard.as_ref().unwrap()) {
            // One needs to unlock (i.e., drop) the read guard to acquire the
            // write guard to perform the load. Otherwise, it'd deadlock.
            drop(guard);

            self.load(key, &mut self.key(key).write().unwrap())?;

            info!("acquiring new read lock to return...");
            guard = self.key(key).read().unwrap();
        }

        Ok(ReadRef(guard))
    }

    /// Acquires the value by the given key, for write.
    pub fn write(&self, key: &S::Key) -> Result<WriteRef<'_, S::Val>, S::Err> {
        info!("acquiring write lock...");
        let mut guard = self.key(key).write().unwrap();
        if guard.is_none() || S::match_kv(key, guard.as_ref().unwrap()) {
            self.load(key, &mut guard)?;
        }
        Ok(WriteRef(guard))
    }

    /// Loads the given page.
    fn load(&self, key: &S::Key, opt: &mut Option<S::Val>) -> Result<(), S::Err> {
        info!("storing new `load result`...");
        opt.replace({
            let mut load_guard = self.strategy.lock().unwrap();
            load_guard.load(key)?
        });
        Ok(())
    }

    /// Returns a copy of the current strategy.
    pub fn clone_strategy(&self) -> S
    where
        S: Clone,
    {
        self.strategy.lock().unwrap().clone()
    }

    /// Returns the inner strategy.
    pub fn into_strategy(self) -> S {
        self.strategy.into_inner().unwrap()
    }
}

/// A read-only shared view over a cache entry's value.
pub struct ReadRef<'a, V>(RwLockReadGuard<'a, Option<V>>);

impl<V> Deref for ReadRef<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

/// a write exclusive view over a cache entry's value.
pub struct WriteRef<'a, V>(RwLockWriteGuard<'a, Option<V>>);

impl<V> Deref for WriteRef<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl<V> DerefMut for WriteRef<'_, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[test]
    fn test_multiple_readers_same_key() {
        let s = TestStrategy::default();
        let c = Cache::<TestStrategy, TestHashBuilder>::new::<4>(s);

        let s1 = c.read(&1).unwrap();
        assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
        let s2 = c.read(&1).unwrap();
        assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);

        assert_eq!(&*s1, &*s2);
    }

    #[test]
    fn test_read_and_write_same_key() {
        let s = TestStrategy::default();
        let c = Cache::<TestStrategy, TestHashBuilder>::new::<4>(s);

        {
            let data = c.read(&1).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
            assert_eq!(&*data, "1one");
        }
        {
            let mut data = c.write(&1).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
            data.push_str("-mod");
        }
        {
            let data = c.read(&1).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
            assert_eq!(&*data, "1one-mod");
        }
    }

    #[test]
    fn test_read_diff_keys() {
        let s = TestStrategy::default();
        let c = Cache::<TestStrategy, TestHashBuilder>::new::<4>(s);

        let s1 = c.write(&1).unwrap();
        assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
        let s2 = c.write(&2).unwrap();
        assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 2);

        assert_ne!(&*s1, &*s2);
    }

    #[test]
    fn test_collision() {
        let s = TestStrategy::default();
        let c = Cache::<TestStrategy, TestHashBuilder>::new::<4>(s);

        {
            let s1 = c.read(&1).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
            assert_eq!(&*s1, "1one");
        }
        {
            // won't change here
            let s1 = c.read(&1).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 1);
            assert_eq!(&*s1, "1one");
        }
        {
            // will change here since `5 % 4 = 1`
            let s2 = c.read(&5).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 2);
            assert_eq!(&*s2, "5five");
        }
        {
            // hence, third load
            let s1 = c.read(&1).unwrap();
            assert_eq!(c.clone_strategy().count.load(Ordering::SeqCst), 3);
            assert_eq!(&*s1, "1one");
        }
    }

    #[derive(Default)]
    struct TestStrategy {
        count: AtomicU32,
    }

    impl Clone for TestStrategy {
        fn clone(&self) -> Self {
            Self {
                count: AtomicU32::new(self.count.load(Ordering::SeqCst)),
            }
        }
    }

    impl CacheStrategy for TestStrategy {
        type Key = u32;
        type Val = String;
        type Err = ();

        fn load(&mut self, key: &Self::Key) -> Result<Self::Val, Self::Err> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(match key {
                1 => "1one",
                2 => "2two",
                3 => "3three",
                4 => "4four",
                5 => "5five",
                _ => "unknown",
            }
            .into())
        }

        fn match_kv(key: &Self::Key, val: &Self::Val) -> bool {
            !val.starts_with(&key.to_string())
        }
    }

    #[derive(Default)]
    struct TestHashBuilder;

    impl BuildHasher for TestHashBuilder {
        type Hasher = TestHasher;

        fn build_hasher(&self) -> Self::Hasher {
            TestHasher(0)
        }
    }

    struct TestHasher(u64);

    impl Hasher for TestHasher {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            let mut arr = [0_u8; 8];
            arr[..4].copy_from_slice(bytes);
            let orig = u64::from_ne_bytes(arr);
            self.0 = orig;
        }
    }
}
