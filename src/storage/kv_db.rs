use sha1::{Sha1, Digest};
use std::collections::BTreeMap;
use std::io::Write;

use super::StorageBackend;
use crate::internals::{Hash, Object, ObjectType};

/// A KV Database Storage Backend stub.
/// This currently uses an in-memory BTreeMap as a mock KV store.
/// It is designed to be easily replaced by `sled` or `rocksdb` in the future.
pub struct KvDatabaseStorage {
    db: BTreeMap<[u8; 20], Vec<u8>>,
}

impl KvDatabaseStorage {
    pub fn new() -> Self {
        Self {
            db: BTreeMap::new(),
        }
    }

    fn serialize_object(&self, obj_type: ObjectType, content: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        // 1 byte for type, followed by content
        let type_byte = match obj_type {
            ObjectType::Commit => 1,
            ObjectType::Tree => 2,
            ObjectType::Blob => 3,
            ObjectType::Tag => 4,
        };
        data.push(type_byte);
        data.extend_from_slice(content);
        data
    }

    fn deserialize_object(&self, data: &[u8]) -> Option<(ObjectType, Box<[u8]>)> {
        if data.is_empty() {
            return None;
        }
        let obj_type = match data[0] {
            1 => ObjectType::Commit,
            2 => ObjectType::Tree,
            3 => ObjectType::Blob,
            4 => ObjectType::Tag,
            _ => return None,
        };
        let content = data[1..].to_vec().into_boxed_slice();
        Some((obj_type, content))
    }
}

impl StorageBackend for KvDatabaseStorage {
    fn hash(&self, obj_type: ObjectType, content: &[u8]) -> Hash {
        let mut hasher = Sha1::new();
        write!(&mut hasher, "{} {}\0", obj_type, content.len()).unwrap();
        hasher.update(content);
        Hash::new(hasher.finalize().into())
    }

    fn insert(&mut self, obj_type: ObjectType, content: Box<[u8]>, _delta_hint: Option<Hash>) -> Hash {
        let hash = self.hash(obj_type, &content);
        let serialized = self.serialize_object(obj_type, &content);
        self.db.insert(hash.to_bytes(), serialized);
        hash
    }

    fn get(&self, object: Hash) -> Option<Object> {
        if let Some(data) = self.db.get(&object.to_bytes()) {
            if let Some((obj_type, content)) = self.deserialize_object(data) {
                return Some(Object {
                    obj_type,
                    content,
                    delta_hint: Hash::zero(),
                });
            }
        }
        None
    }

    fn has(&self, object: Hash) -> bool {
        self.db.contains_key(&object.to_bytes())
    }

    fn get_as(&self, object: Hash, obj_type: ObjectType) -> Option<Box<[u8]>> {
        if let Some(data) = self.db.get(&object.to_bytes()) {
            if let Some((parsed_type, content)) = self.deserialize_object(data) {
                if parsed_type == obj_type {
                    return Some(content);
                } else {
                    log::warn!("Object {} was expected to be a {:?} but it's actually a {:?}", object, obj_type, parsed_type);
                }
            }
        }
        None
    }

    fn remove(&mut self, object: Hash) -> Option<Object> {
        if let Some(data) = self.db.remove(&object.to_bytes()) {
            if let Some((obj_type, content)) = self.deserialize_object(&data) {
                return Some(Object {
                    obj_type,
                    content,
                    delta_hint: Hash::zero(),
                });
            }
        }
        None
    }
}
