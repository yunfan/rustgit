pub mod memory;
pub mod git_disk;
pub mod kv_db;
pub mod state;

pub use self::state::StateStore;

use super::internals::{Hash, Object, ObjectType};

pub trait StorageBackend: Send + Sync {
    /// Hashes the content without inserting
    fn hash(&self, obj_type: ObjectType, content: &[u8]) -> Hash;

    /// Inserts an object into the storage
    fn insert(&mut self, obj_type: ObjectType, content: Box<[u8]>, delta_hint: Option<Hash>) -> Hash;

    /// Gets an object from the storage
    fn get(&self, object: Hash) -> Option<Object>;

    /// Checks if the storage contains an object
    fn has(&self, object: Hash) -> bool;

    /// Gets the content of an object as bytes, checking its type
    fn get_as(&self, object: Hash, obj_type: ObjectType) -> Option<Box<[u8]>>;

    /// Removes an object from the storage
    fn remove(&mut self, object: Hash) -> Option<Object>;
}
