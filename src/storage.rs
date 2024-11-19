use std::collections::HashMap;
use std::sync::{RwLock, RwLockWriteGuard};
use std::time::SystemTime;

type BinaryData = Vec<u8>;
pub(crate) type StorageKey = BinaryData;
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

    pub(crate) fn get_simple(&self, key: &StorageKey) -> Option<SimpleValue> {
        let guard = self.inner.read().expect("got poisoned lock, can't handle that");
        let Some(StorageItem::Simple(item)) = guard.get(key) else {
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
            StorageItem::Simple(x) => if x.is_expired() {
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

    pub(crate) fn set_string(&self, key: Vec<u8>, item: StorageItemSimple) -> RwLockWriteGuard<StorageInner> {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        guard.insert(key, StorageItem::Simple(item));
        guard
    }

    pub(crate) fn increment(&self, key: Vec<u8>) -> Option<(RwLockWriteGuard<StorageInner>, i64)> {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        let entry = guard.entry(key)
            .or_insert_with(|| StorageItem::Simple(StorageItemSimple{ value: SimpleValue::Int(0), expires_at: None }));
        let value = match entry {
            StorageItem::Simple(x) => match &mut x.value {
                SimpleValue::Int(x) => x,
                _ => return None,
            },
            _ => return None,
        };
        *value += 1;
        let copy = *value;
        Some((guard, copy))
    }
    
    pub(crate) fn append_to_stream(&self, key: Vec<u8>, item: StreamEntry) -> Option<RwLockWriteGuard<StorageInner>> {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        let entry = guard.entry(key)
            .or_insert_with(|| StorageItem::Stream(Default::default()));
        let stream = match entry {
            StorageItem::Stream(x) => x,
            _ => return None,
        };
        stream.push(item);
        Some(guard)
    }

    pub(crate) fn delete_expired(&self, key: &StorageKey) {
        let mut guard = self.inner.write().expect("got poisoned lock, can't handle that");
        let Some(StorageItem::Simple(item)) = guard.get(key) else {
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
    Simple(StorageItemSimple),
    Stream(StorageItemStream),
}

#[derive(Clone, Debug)]
pub(crate) struct StorageItemSimple {
    pub value: SimpleValue,
    pub expires_at: Option<ExpiryTs>,
}
impl StorageItemSimple {
    pub fn from_data(value: Vec<u8>, expires_at: Option<ExpiryTs>) -> Self {
        let value = match get_int_value(&value) {
            Some(x) => SimpleValue::Int(x),
            None => SimpleValue::String(value),
        };
        StorageItemSimple { value, expires_at }
    }
    pub fn is_expired(&self) -> bool {
        let Some(expires_at) = self.expires_at else {
            return false;
        };
        expires_at < now_ts()
    }
}
fn get_int_value(value: &[u8]) -> Option<i64> {
    let utf = std::str::from_utf8(value).ok()?;
    let int = utf.parse().ok()?;
    Some(int)
}

#[derive(Clone, Debug)]
pub(crate) enum SimpleValue {
    String(BinaryData),
    Int(i64),
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
    #[allow(dead_code)]
    pub data: HashMap<StorageKey, BinaryData>,
}
pub(crate) type StreamEntryId = Vec<u8>;
