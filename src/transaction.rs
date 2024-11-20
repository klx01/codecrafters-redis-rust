use crate::command::Command;
use crate::storage::{StorageItemSimple, StorageKey, StreamEntry};

#[derive(Default, Debug)]
pub(crate) struct Transaction {
    pub started: bool,
    pub queue: Vec<QueuedCommand>,
}

#[derive(Debug)]
pub(crate) enum QueuedCommand {
    // todo: think of some better way to replicate commands without saving them here
    Set{key: StorageKey, item: StorageItemSimple, command: Command},
    Xadd{key: StorageKey, item: StreamEntry, command: Command},
    Incr{key: StorageKey, command: Command},
    Get{key: StorageKey},
}