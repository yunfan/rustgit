use std::fs::{self, File};
use std::io::{self, Read, Write};
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

pub struct GitDiskStateStore {
    repo_path: PathBuf,
}

impl GitDiskStateStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self { repo_path: path.as_ref().to_path_buf() }
    }
}

impl super::StateStore for GitDiskStateStore {
    fn read_ref(&self, name: &str) -> Option<Hash> {
        let path = self.repo_path.join(".git").join(name);
        if let Ok(content) = fs::read_to_string(path) {
            Hash::from_hex(content.trim())
        } else {
            None
        }
    }

    fn write_ref(&mut self, name: &str, hash: Hash) -> io::Result<()> {
        let path = self.repo_path.join(".git").join(name);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = File::create(path)?;
        writeln!(file, "{}", hash)?;
        Ok(())
    }

    fn delete_ref(&mut self, name: &str) -> io::Result<()> {
        let path = self.repo_path.join(".git").join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn list_refs(&self, prefix: &str) -> io::Result<Vec<(String, Hash)>> {
        let mut refs = Vec::new();
        let base_path = self.repo_path.join(".git").join(prefix);
        if base_path.exists() {
            let mut stack = vec![base_path.clone()];
            while let Some(dir) = stack.pop() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if path.is_file() {
                        if let Ok(content) = fs::read_to_string(&path) {
                            if let Some(hash) = Hash::from_hex(content.trim()) {
                                if let Ok(rel_path) = path.strip_prefix(&self.repo_path.join(".git")) {
                                    refs.push((rel_path.to_string_lossy().to_string(), hash));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(refs)
    }

    fn read_head(&self) -> Option<String> {
        let path = self.repo_path.join(".git").join("HEAD");
        fs::read_to_string(path).ok().map(|s| s.trim().to_string())
    }

    fn write_head(&mut self, target: &str) -> io::Result<()> {
        let path = self.repo_path.join(".git").join("HEAD");
        let mut file = File::create(path)?;
        writeln!(file, "{}", target)?;
        Ok(())
    }

    fn read_index(&self) -> Option<String> {
        let mut files = Vec::new();

        // Read standard git binary index (dircache V2/V3)
        let git_index_path = self.repo_path.join(".git").join("index");
        if let Ok(data) = fs::read(git_index_path) {
            if data.len() >= 12 && &data[0..4] == b"DIRC" {
                if let Ok(version) = data[4..8].try_into().map(u32::from_be_bytes) {
                    if version == 2 || version == 3 {
                        if let Ok(count) = data[8..12].try_into().map(u32::from_be_bytes) {
                            let mut offset = 12;
                            for _ in 0..count {
                                if offset + 62 > data.len() { break; }
                                if let Ok(flags) = data[offset+60..offset+62].try_into().map(u16::from_be_bytes) {
                                    let name_len = (flags & 0xFFF) as usize;
                                    offset += 62;
                                    let name_end = offset + name_len;
                                    if name_end > data.len() { break; }
                                    
                                    if let Ok(name) = std::str::from_utf8(&data[offset..name_end]) {
                                        if !files.contains(&name.to_string()) {
                                            files.push(name.to_string());
                                        }
                                    }
                                    
                                    let total_len = 62 + name_len;
                                    let pad = 8 - (total_len % 8);
                                    offset += name_len + pad;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        if files.is_empty() {
            None
        } else {
            Some(files.join("\n"))
        }
    }

    fn write_index(&mut self, data: &str) -> io::Result<()> {
        // Write binary standard .git/index to perfectly sync with native Git
        // Even if we use fake zeroes for stat metadata, native Git will recognize it
        use sha1::{Sha1, Digest};
        let mut index = Vec::new();
        index.extend_from_slice(b"DIRC");
        index.extend_from_slice(&2u32.to_be_bytes());

        let mut lines: Vec<&str> = data.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
        lines.sort();
        index.extend_from_slice(&(lines.len() as u32).to_be_bytes());

        for line in lines {
            let file_path = self.repo_path.join(line);
            if let Ok(content) = fs::read(&file_path) {
                let mut entry = Vec::new();
                entry.extend_from_slice(&0u32.to_be_bytes()); // ctime.sec
                entry.extend_from_slice(&0u32.to_be_bytes()); // ctime.nsec
                entry.extend_from_slice(&0u32.to_be_bytes()); // mtime.sec
                entry.extend_from_slice(&0u32.to_be_bytes()); // mtime.nsec
                entry.extend_from_slice(&0u32.to_be_bytes()); // dev
                entry.extend_from_slice(&0u32.to_be_bytes()); // ino
                entry.extend_from_slice(&0x81A4u32.to_be_bytes()); // mode 100644
                entry.extend_from_slice(&0u32.to_be_bytes()); // uid
                entry.extend_from_slice(&0u32.to_be_bytes()); // gid
                entry.extend_from_slice(&(content.len() as u32).to_be_bytes()); // size

                let mut hasher = Sha1::new();
                hasher.update(format!("blob {}\0", content.len()).as_bytes());
                hasher.update(&content);
                let hash = hasher.finalize();
                entry.extend_from_slice(&hash);

                let flags = (line.len() as u16) & 0xFFF;
                entry.extend_from_slice(&flags.to_be_bytes());
                entry.extend_from_slice(line.as_bytes());

                let total_len = 62 + line.len();
                let pad = 8 - (total_len % 8);
                for _ in 0..pad {
                    entry.push(0);
                }
                index.extend_from_slice(&entry);
            }
        }

        let mut hasher = Sha1::new();
        hasher.update(&index);
        let index_hash = hasher.finalize();
        index.extend_from_slice(&index_hash);

        let git_index_path = self.repo_path.join(".git").join("index");
        let mut git_index_file = fs::OpenOptions::new().create(true).write(true).truncate(true).open(git_index_path)?;
        git_index_file.write_all(&index)?;

        Ok(())
    }

    fn clear_index(&mut self) -> io::Result<()> {
        let git_index_path = self.repo_path.join(".git").join("index");
        if git_index_path.exists() {
            fs::remove_file(git_index_path)?;
        }
        
        Ok(())
    }
}
