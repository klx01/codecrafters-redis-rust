use std::collections::HashMap;
use std::sync::{RwLock, RwLockWriteGuard};
use std::time::SystemTime;

type StorageValue = Vec<u8>;
pub(crate) type StorageKey = Vec<u8>;
pub(crate) type StorageInner = HashMap<StorageKey, StorageItem>;
pub(crate) type ExpiryTs = u128;

#[derive(Default)]
pub(crate) struct Storage {
    inner: RwLock<StorageInner>
}
impl Storage {
    pub(crate) fn new(inner: StorageInner) -> Self {
        Self{ inner: RwLock::new(inner) }
    }
    
    pub(crate) fn get(&self, key: &StorageKey) -> Option<StorageValue> {
        let guard = self.inner.read().expect("got poisoned lock, can't handle that");
        let Some(item) = guard.get(key) else {
            return None;
        };
        if item.is_expired() {
            drop(guard);
            self.delete_expired(key);
            return None;
        }
        return Some(item.value.clone());
    }
    
    pub(crate) fn keys(&self) -> Vec<StorageKey> {
        let guard = self.inner.read().expect("got poisoned lock, can't handle that");
        return guard.keys().cloned().collect();
    }

    pub(crate) fn set(&self, key: Vec<u8>, item: StorageItem) -> RwLockWriteGuard<StorageInner> {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        guard.insert(key, item);
        guard
    }

    pub(crate) fn delete_expired(&self, key: &StorageKey) {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        let Some(item) = guard.get(key) else {
            return;
        };
        if !item.is_expired() {
            return;
        }
        guard.remove(key);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StorageItem {
    pub value: StorageValue,
    pub expires_at: Option<ExpiryTs>,
}
impl StorageItem {
    pub fn is_expired(&self) -> bool {
        let Some(expires_at) = self.expires_at else {
            return false;
        };
        expires_at < now_ts()
    }
}

pub(crate) fn now_ts() -> ExpiryTs {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
        .expect("failed to get timestamp!")
        .as_millis()
}