use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

type StorageValue = Vec<u8>;
type StorageKey = Vec<u8>;

#[derive(Default)]
pub(crate) struct Storage {
    inner: RwLock<HashMap<StorageKey, StorageItem>>
}
impl Storage {
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

    pub(crate) fn set(&self, key: Vec<u8>, item: StorageItem) {
        self.inner.write().expect("got poisoned lock, can't handle that")
            .insert(key, item);
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

pub(crate) struct StorageItem {
    pub value: StorageValue,
    pub expires_at: Option<Instant>,
}
impl StorageItem {
    pub fn is_expired(&self) -> bool {
        let Some(expires_at) = self.expires_at else {
            return false;
        };
        return expires_at < Instant::now();
    }
}