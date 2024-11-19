use crate::storage::{StorageItemSimple, StorageKey, StreamEntry};

#[derive(Default, Debug)]
pub(crate) struct Transaction {
    pub started: bool,
    pub queue: Vec<QueuedCommand>,
}
impl Transaction {

}

#[derive(Debug)]
pub(crate) enum QueuedCommand {
    Set{key: StorageKey, item: StorageItemSimple},
    Xadd{key: StorageKey, item: StreamEntry},
    Incr{key: StorageKey},
}