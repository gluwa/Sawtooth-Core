use log::error;
use std::ops::{Deref, DerefMut};
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use std::thread::current;

#[derive(Default)]
pub struct IRwLock<T> {
    inner: RwLock<T>,
    name: String,
}

pub struct IRwLockReadGuard<'a, T> {
    inner: RwLockReadGuard<'a, T>,
    lock: &'a IRwLock<T>,
    place: String,
}

impl<T> Drop for IRwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        error!(
            "θ;{};Rel;{};{};{}",
            self.lock.name,
            'R',
            self.lock.thread(),
            self.place
        );
    }
}

impl<T> Deref for IRwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct IRwLockWriteGuard<'a, T> {
    inner: RwLockWriteGuard<'a, T>,
    lock: &'a IRwLock<T>,
    place: String,
}

impl<T> DerefMut for IRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<'a, T> Drop for IRwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        error!(
            "θ;{};Rel;{};{};{}",
            self.lock.name,
            'W',
            self.lock.thread(),
            self.place
        );
    }
}

impl<T> Deref for IRwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> IRwLock<T> {
    pub fn new(name: String, t: T) -> IRwLock<T> {
        IRwLock {
            inner: RwLock::new(t),
            name,
        }
    }

    fn thread(&self) -> String {
        current()
            .name()
            .map(|s| s.into())
            .unwrap_or("<unknown>".into())
    }

    pub fn read(
        &self,
        place: &str,
    ) -> Result<IRwLockReadGuard<T>, PoisonError<RwLockReadGuard<T>>> {
        error!("θ;{};Wait;{};{};{}", self.name, 'R', self.thread(), place);
        let inner = self.inner.read()?;
        error!("θ;{};Acq;{};{};{}", self.name, 'R', self.thread(), place);
        Ok(IRwLockReadGuard {
            inner,
            lock: self,
            place: place.to_string(),
        })
    }

    pub fn write(
        &self,
        place: &str,
    ) -> Result<IRwLockWriteGuard<T>, PoisonError<RwLockWriteGuard<T>>> {
        error!("θ;{};Wait;{};{};{}", self.name, 'W', self.thread(), place);
        //newtype guard
        let inner = self.inner.write()?;
        error!("θ;{};Acq;{};{};{}", self.name, 'W', self.thread(), place);
        Ok(IRwLockWriteGuard {
            inner,
            lock: self,
            place: place.to_string(),
        })
    }
}
//create new type for ilock with the same interface as python's
//pub type IRwLock<T> = RwLock<T>;

/*

class ILock:
    def __init__(self, name, lock):
        self._name = name
        self._lock = lock()

    def thread(self):
        return threading.current_thread().getName()

    def __enter__(self):
        self._lock.acquire()
        LOGGER.warning("θ;%s;Acq;%s", self._name, self.thread())

    def __exit__(self, exc_type, exc_value, traceback):
        LOGGER.warning("θ;%s;Rel;%s", self._name, self.thread())
        self._lock.release()

    def __call__(self, place: str = None):
        if place:
            LOGGER.warning("θ;%s;Wait;%s;%s", self._name, self.thread(), place)
        return self
 */
