use core::array::from_fn;
use lmfu::LiteMap;
use sha1::{Sha1, Digest};
use std::io::Write;

use super::StorageBackend;
use crate::internals::{Hash, Object, ObjectType};

pub struct MemoryStorage([LiteMap<Hash, Object>; 256]);

impl MemoryStorage {
    pub fn new() -> Self {
        Self(from_fn(|_| LiteMap::new()))
    }
}

impl StorageBackend for MemoryStorage {
    fn hash(&self, obj_type: ObjectType, content: &[u8]) -> Hash {
        let mut hasher = Sha1::new();
        write!(&mut hasher, "{} {}\0", obj_type, content.len()).unwrap();
        hasher.update(content);
        Hash::new(hasher.finalize().into())
    }

    fn insert(&mut self, obj_type: ObjectType, content: Box<[u8]>, delta_hint: Option<Hash>) -> Hash {
        let delta_hint = delta_hint.unwrap_or(Hash::zero());
        let hash = self.hash(obj_type, &content);
        let entry = Object {
            obj_type,
            content,
            delta_hint,
        };
        self.0[hash.to_bytes()[0] as usize].insert(hash, entry);
        hash
    }

    fn get(&self, object: Hash) -> Option<Object> {
        self.0[object.to_bytes()[0] as usize].get(&object).cloned()
    }

    fn has(&self, object: Hash) -> bool {
        self.0[object.to_bytes()[0] as usize].contains_key(&object)
    }

    fn get_as(&self, object: Hash, obj_type: ObjectType) -> Option<Box<[u8]>> {
        match self.0[object.to_bytes()[0] as usize].get(&object) {
            Some(entry) => match entry.obj_type == obj_type {
                true => Some(entry.content.clone()),
                false => {
                    log::warn!("Object {} was expected to be a {:?} but it's actually a {:?}", object, obj_type, entry.obj_type);
                    None
                },
            },
            None => None,
        }
    }

    fn remove(&mut self, object: Hash) -> Option<Object> {
        self.0[object.to_bytes()[0] as usize].remove(&object)
    }
}

use std::sync::{Arc, RwLock};
use std::io;

#[derive(Clone, Default)]
pub struct MemoryStateStore {
    refs: Arc<RwLock<std::collections::HashMap<String, Hash>>>,
    head: Arc<RwLock<Option<String>>>,
    index: Arc<RwLock<Option<String>>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl super::StateStore for MemoryStateStore {
    fn read_ref(&self, name: &str) -> Option<Hash> {
        self.refs.read().unwrap().get(name).copied()
    }

    fn write_ref(&mut self, name: &str, hash: Hash) -> io::Result<()> {
        self.refs.write().unwrap().insert(name.to_string(), hash);
        Ok(())
    }

    fn delete_ref(&mut self, name: &str) -> io::Result<()> {
        self.refs.write().unwrap().remove(name);
        Ok(())
    }

    fn list_refs(&self, prefix: &str) -> io::Result<Vec<(String, Hash)>> {
        let refs = self.refs.read().unwrap();
        let mut result = Vec::new();
        for (name, hash) in refs.iter() {
            if let Some(rel) = name.strip_prefix(prefix) {
                result.push((rel.to_string(), *hash));
            }
        }
        Ok(result)
    }

    fn read_head(&self) -> Option<String> {
        self.head.read().unwrap().clone()
    }

    fn write_head(&mut self, target: &str) -> io::Result<()> {
        *self.head.write().unwrap() = Some(target.to_string());
        Ok(())
    }

    fn read_index(&self) -> Option<String> {
        self.index.read().unwrap().clone()
    }

    fn write_index(&mut self, data: &str) -> io::Result<()> {
        *self.index.write().unwrap() = Some(data.to_string());
        Ok(())
    }

    fn clear_index(&mut self) -> io::Result<()> {
        *self.index.write().unwrap() = None;
        Ok(())
    }
}
