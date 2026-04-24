use sha1::{Sha1, Digest};
use std::io::Write;

use super::StorageBackend;
use crate::internals::{Hash, Object, ObjectType};

/// A KV Database Storage Backend stub.
/// This currently uses an in-memory BTreeMap as a mock KV store.
/// It is designed to be easily replaced by `sled` or `rocksdb` in the future.
pub struct KvDatabaseStorage {
    db: sled::Db,
}

impl KvDatabaseStorage {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
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
        let _ = self.db.insert(hash.to_bytes(), serialized);
        let _ = self.db.flush();
        hash
    }

    fn get(&self, object: Hash) -> Option<Object> {
        if let Ok(Some(data)) = self.db.get(&object.to_bytes()) {
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

    fn has(&self, object: Hash) -> bool {
        self.db.contains_key(&object.to_bytes()).unwrap_or(false)
    }

    fn get_as(&self, object: Hash, obj_type: ObjectType) -> Option<Box<[u8]>> {
        if let Ok(Some(data)) = self.db.get(&object.to_bytes()) {
            if let Some((parsed_type, content)) = self.deserialize_object(&data) {
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
        if let Ok(Some(data)) = self.db.remove(&object.to_bytes()) {
            let _ = self.db.flush();
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

pub struct KvStateStore {
    db: sled::Db,
}

impl KvStateStore {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }
}

impl super::StateStore for KvStateStore {
    fn read_ref(&self, name: &str) -> Option<Hash> {
        let key = format!("ref:{}", name);
        if let Ok(Some(value)) = self.db.get(key.as_bytes()) {
            if let Ok(hash_str) = std::str::from_utf8(&value) {
                return Hash::from_hex(hash_str);
            }
        }
        None
    }

    fn write_ref(&mut self, name: &str, hash: Hash) -> std::io::Result<()> {
        let key = format!("ref:{}", name);
        let _ = self.db.insert(key.as_bytes(), hash.to_string().as_bytes());
        let _ = self.db.flush();
        Ok(())
    }

    fn delete_ref(&mut self, name: &str) -> std::io::Result<()> {
        let key = format!("ref:{}", name);
        let _ = self.db.remove(key.as_bytes());
        let _ = self.db.flush();
        Ok(())
    }

    fn list_refs(&self, prefix: &str) -> std::io::Result<Vec<(String, Hash)>> {
        let mut refs = Vec::new();
        let prefix_key = format!("ref:{}", prefix);
        for item in self.db.scan_prefix(prefix_key.as_bytes()) {
            if let Ok((key, value)) = item {
                if let Ok(key_str) = std::str::from_utf8(&key) {
                    if let Some(rel_path) = key_str.strip_prefix("ref:") {
                        if let Ok(hash_str) = std::str::from_utf8(&value) {
                            if let Some(hash) = Hash::from_hex(hash_str) {
                                refs.push((rel_path.to_string(), hash));
                            }
                        }
                    }
                }
            }
        }
        Ok(refs)
    }

    fn read_head(&self) -> Option<String> {
        if let Ok(Some(value)) = self.db.get(b"HEAD") {
            std::str::from_utf8(&value).map(|s| s.to_string()).ok()
        } else {
            None
        }
    }

    fn write_head(&mut self, target: &str) -> std::io::Result<()> {
        let _ = self.db.insert(b"HEAD", target.as_bytes());
        let _ = self.db.flush();
        Ok(())
    }

    fn read_index(&self) -> Option<String> {
        if let Ok(Some(value)) = self.db.get(b"INDEX") {
            std::str::from_utf8(&value).map(|s| s.to_string()).ok()
        } else {
            None
        }
    }

    fn write_index(&mut self, data: &str) -> std::io::Result<()> {
        let _ = self.db.insert(b"INDEX", data.as_bytes());
        let _ = self.db.flush();
        Ok(())
    }

    fn clear_index(&mut self) -> std::io::Result<()> {
        let _ = self.db.remove(b"INDEX");
        let _ = self.db.flush();
        Ok(())
    }
}
