use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use sha1::{Sha1, Digest};

use super::StorageBackend;
use crate::internals::{Hash, Object, ObjectType};
use miniz_oxide::deflate::compress_to_vec_zlib;
use miniz_oxide::inflate::decompress_to_vec_zlib;

pub struct GitDiskStorage {
    repo_path: PathBuf,
}

impl GitDiskStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        Ok(Self { repo_path: path })
    }

    pub fn init_on_disk<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let repo_path = path.as_ref().to_path_buf();
        let git_dir = repo_path.join(".git");
        
        fs::create_dir_all(git_dir.join("objects"))?;
        fs::create_dir_all(git_dir.join("refs/heads"))?;
        fs::create_dir_all(git_dir.join("refs/tags"))?;

        // Write HEAD
        let mut head_file = File::create(git_dir.join("HEAD"))?;
        head_file.write_all(b"ref: refs/heads/master\n")?;

        // Write config
        let mut config_file = File::create(git_dir.join("config"))?;
        config_file.write_all(b"[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = false\n")?;

        Ok(Self { repo_path })
    }

    fn object_path(&self, hash: Hash) -> PathBuf {
        let hex = hash.to_string();
        let (dir, file) = hex.split_at(2);
        self.repo_path.join(".git").join("objects").join(dir).join(file)
    }
}

impl StorageBackend for GitDiskStorage {
    fn hash(&self, obj_type: ObjectType, content: &[u8]) -> Hash {
        let mut hasher = Sha1::new();
        write!(&mut hasher, "{} {}\0", obj_type, content.len()).unwrap();
        hasher.update(content);
        Hash::new(hasher.finalize().into())
    }

    fn insert(&mut self, obj_type: ObjectType, content: Box<[u8]>, _delta_hint: Option<Hash>) -> Hash {
        let hash = self.hash(obj_type, &content);
        let path = self.object_path(hash);

        if !path.exists() {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let mut header = Vec::new();
            write!(&mut header, "{} {}\0", obj_type, content.len()).unwrap();
            header.extend_from_slice(&content);

            let compressed = compress_to_vec_zlib(&header, 6);
            if let Ok(mut file) = File::create(&path) {
                let _ = file.write_all(&compressed);
            }
        }
        hash
    }

    fn get(&self, object: Hash) -> Option<Object> {
        let path = self.object_path(object);
        if let Ok(mut file) = File::open(&path) {
            let mut compressed = Vec::new();
            if file.read_to_end(&mut compressed).is_ok() {
                if let Ok(decompressed) = decompress_to_vec_zlib(&compressed) {
                    if let Some(null_idx) = decompressed.iter().position(|&b| b == 0) {
                        let header = std::str::from_utf8(&decompressed[..null_idx]).unwrap_or("");
                        let obj_type = if header.starts_with("commit ") {
                            ObjectType::Commit
                        } else if header.starts_with("tree ") {
                            ObjectType::Tree
                        } else if header.starts_with("blob ") {
                            ObjectType::Blob
                        } else if header.starts_with("tag ") {
                            ObjectType::Tag
                        } else {
                            return None;
                        };
                        
                        let content = decompressed[null_idx + 1..].to_vec().into_boxed_slice();
                        return Some(Object {
                            obj_type,
                            content,
                            delta_hint: Hash::zero(),
                        });
                    }
                }
            }
        }
        None
    }

    fn has(&self, object: Hash) -> bool {
        self.object_path(object).exists()
    }

    fn get_as(&self, object: Hash, obj_type: ObjectType) -> Option<Box<[u8]>> {
        if let Some(obj) = self.get(object) {
            if obj.obj_type == obj_type {
                return Some(obj.content);
            } else {
                log::warn!("Object {} was expected to be a {:?} but it's actually a {:?}", object, obj_type, obj.obj_type);
            }
        }
        None
    }

    fn remove(&mut self, object: Hash) -> Option<Object> {
        let obj = self.get(object);
        if obj.is_some() {
            let _ = fs::remove_file(self.object_path(object));
        }
        obj
    }
}
