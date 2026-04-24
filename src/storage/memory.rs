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
