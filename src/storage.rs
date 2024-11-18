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

    pub(crate) fn get_string(&self, key: &StorageKey) -> Option<StorageValue> {
        let guard = self.inner.read().expect("got poisoned lock, can't handle that");
        let Some(StorageItem::String(item)) = guard.get(key) else {
            return None;
        };
        if item.is_expired() {
            drop(guard);
            self.delete_expired(key);
            return None;
        }
        return Some(item.value.clone());
    }

    pub(crate) fn get_value_kind(&self, key: &StorageKey) -> &'static str {
        let guard = self.inner.read().expect("got poisoned lock, can't handle that");
        let Some(item) = guard.get(key) else {
            return "none";
        };
        match item {
            StorageItem::String(x) => if x.is_expired() {
                "none"
            } else {
                "string"
            },
            StorageItem::Stream(_) => "stream",
        }
    }

    pub(crate) fn keys(&self) -> Vec<StorageKey> {
        let guard = self.inner.read().expect("got poisoned lock, can't handle that");
        return guard.keys().cloned().collect();
    }

    pub(crate) fn set_string(&self, key: Vec<u8>, item: StorageItemString) -> RwLockWriteGuard<StorageInner> {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        guard.insert(key, StorageItem::String(item));
        guard
    }
    
    pub(crate) fn append_to_stream(&self, key: Vec<u8>, item: StreamEntry) -> Option<RwLockWriteGuard<StorageInner>> {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        let entry = guard.entry(key).or_insert(StorageItem::Stream(Default::default()));
        let stream = match entry {
            StorageItem::Stream(x) => x,
            _ => return None,
        };
        stream.push(item);
        Some(guard)
    }

    pub(crate) fn delete_expired(&self, key: &StorageKey) {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        let Some(StorageItem::String(item)) = guard.get(key) else {
            return;
        };
        if !item.is_expired() {
            return;
        }
        guard.remove(key);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum StorageItem {
    String(StorageItemString),
    Stream(StorageItemStream),
}

#[derive(Clone, Debug)]
pub(crate) struct StorageItemString {
    pub value: StorageValue,
    pub expires_at: Option<ExpiryTs>,
}
impl StorageItemString {
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

pub(crate) type StorageItemStream = Vec<StreamEntry>;
#[derive(Clone, Debug)]
pub(crate) struct StreamEntry {
    pub id: StreamEntryId,
    pub data: HashMap<StorageKey, StorageValue>,
}
pub(crate) type StreamEntryId = Vec<u8>;
