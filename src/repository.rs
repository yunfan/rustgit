
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::RwLock;
use lmfu::LiteMap;

use super::internals::{
    Error, Hash, ObjectType, Result, StorageBackend, StateStore,
    CommitField, get_commit_field_hash, serialize_directory, Directory, Path, TreeIter, FileType, EntryType, Write, Mode,
};

/// Local repository residing in memory or other backend
pub struct Repository {
    pub(crate) directories: RwLock<LiteMap<Hash, Directory>>,
    pub(crate) objects: Box<dyn StorageBackend>,
    pub(crate) states: Box<dyn StateStore>,
    pub(crate) staged: Box<dyn StorageBackend>,
    pub(crate) upstream_head: Hash,
    pub(crate) head: Hash,
    pub(crate) root: Option<Hash>,
}

impl Repository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self {
            directories: RwLock::new(LiteMap::new()),
            objects: Box::new(crate::storage::memory::MemoryStorage::new()),
            states: Box::new(crate::storage::memory::MemoryStateStore::new()),
            staged: Box::new(crate::storage::memory::MemoryStorage::new()),
            upstream_head: Hash::zero(),
            head: Hash::zero(),
            root: None,
        }
    }

    /// Initializes a new repository on disk (Git compatible)
    pub fn init_disk<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        use crate::storage::git_disk::{GitDiskStorage, GitDiskStateStore};
        let storage = GitDiskStorage::init_on_disk(&path).map_err(|_| Error::PathError)?;
        let states = GitDiskStateStore::new(&path);
        Ok(Self {
            directories: RwLock::new(LiteMap::new()),
            objects: Box::new(storage),
            states: Box::new(states),
            staged: Box::new(crate::storage::memory::MemoryStorage::new()),
            upstream_head: Hash::zero(),
            head: Hash::zero(),
            root: None,
        })
    }

    /// Opens an existing repository on disk
    pub fn open_disk<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        use crate::storage::git_disk::{GitDiskStorage, GitDiskStateStore};
        let storage = GitDiskStorage::new(&path).map_err(|_| Error::PathError)?;
        let states = GitDiskStateStore::new(&path);
        Ok(Self {
            directories: RwLock::new(LiteMap::new()),
            objects: Box::new(storage),
            states: Box::new(states),
            staged: Box::new(crate::storage::memory::MemoryStorage::new()),
            upstream_head: Hash::zero(),
            head: Hash::zero(),
            root: None,
        })
    }

    /// Sets the current HEAD hash manually (useful for CLI state restoration)
    pub fn set_head(&mut self, hash: Hash) {
        self.head = hash;
    }

    pub fn read_ref(&self, name: &str) -> Option<Hash> { self.states.read_ref(name) }
    pub fn write_ref(&mut self, name: &str, hash: Hash) -> Result<()> { self.states.write_ref(name, hash).map_err(|_| Error::PathError) }
    pub fn delete_ref(&mut self, name: &str) -> Result<()> { self.states.delete_ref(name).map_err(|_| Error::PathError) }
    pub fn list_refs(&self, prefix: &str) -> Result<Vec<(String, Hash)>> { self.states.list_refs(prefix).map_err(|_| Error::PathError) }
    pub fn read_head_str(&self) -> Option<String> { self.states.read_head() }
    pub fn write_head_str(&mut self, target: &str) -> Result<()> { self.states.write_head(target).map_err(|_| Error::PathError) }
    pub fn read_index(&self) -> Option<String> { self.states.read_index() }
    pub fn write_index(&mut self, data: &str) -> Result<()> { self.states.write_index(data).map_err(|_| Error::PathError) }
    pub fn clear_index(&mut self) -> Result<()> { self.states.clear_index().map_err(|_| Error::PathError) }

    /// Gets an object from the repository's active storage backend
    pub fn get_object(&self, hash: Hash, obj_type: ObjectType) -> Option<Box<[u8]>> {
        self.any_store_get(hash, obj_type)
    }

    /// Initializes a repository with KV Database storage
    pub fn init_kv() -> Self {
        use crate::storage::kv_db::{KvDatabaseStorage, KvStateStore};
        let db = sled::open(".rustgit_kv").unwrap();
        let storage = KvDatabaseStorage::new(db.clone());
        let states = KvStateStore::new(db);
        Self {
            directories: RwLock::new(LiteMap::new()),
            objects: Box::new(storage),
            states: Box::new(states),
            staged: Box::new(crate::storage::memory::MemoryStorage::new()),
            upstream_head: Hash::zero(),
            head: Hash::zero(),
            root: None,
        }
    }

    pub (crate) fn any_store_get(&self, hash: Hash, obj_type: ObjectType) -> Option<Box<[u8]>> {
        match self.staged.get_as(hash, obj_type) {
            Some(entries) => Some(entries),
            None => self.objects.get_as(hash, obj_type),
        }
    }

    /// None = MissingObject for this hash
    pub(crate) fn try_find_dir(&self, hash: Hash) -> Result<Option<Directory>> {
        let entries = match self.any_store_get(hash, ObjectType::Tree) {
            Some(entries) => entries,
            None => return Ok(None),
        };
        let mut iter = TreeIter::new(&entries);

        let mut dir = Directory::new();

        while let Some((node, hash, mode)) = iter.next()? {
            dir.insert(node.into(), (hash, mode));
        }

        Ok(Some(dir))
    }

    pub(crate) fn find_dir(&self, hash: Hash) -> Result<Directory> {
        let dir = self.try_find_dir(hash)?;
        
        if dir.is_none() {
            log::warn!("Missing directory for hash {}", hash);
        }

        Ok(dir.unwrap_or(Directory::new()))
    }

    pub(crate) fn remove_dir(&mut self, dir_hash: Hash) -> Result<Directory> {
        let dirs_mut = self.directories.get_mut().unwrap();
        match dirs_mut.remove(&dir_hash) {
            Some(dir) => Ok(dir),
            None => self.find_dir(dir_hash),
        }
    }

    pub(crate) fn fetch_dir(&self, hash: Hash) -> Result<()> {
        let present = {
            let dirs = self.directories.read().unwrap();
            dirs.contains_key(&hash)
        };

        if !present {
            let dir = self.try_find_dir(hash)?.ok_or(Error::MissingObject)?;
            let mut dirs_mut = self.directories.write().unwrap();
            dirs_mut.insert(hash, dir);
        }

        Ok(())
    }

    pub(crate) fn get_commit_root(&self, commit_hash: Hash) -> Result<Option<Hash>> {
        let commit = self.any_store_get(commit_hash, ObjectType::Commit).ok_or(Error::MissingObject)?;
        match get_commit_field_hash(&commit, CommitField::Tree)? {
            Some(hash) => Ok(Some(hash)),
            None => Err(Error::InvalidObject),
        }
    }

    pub(crate) fn find_in_dir(&self, dir: Hash, node: &str, filter: EntryType) -> Result<(Hash, Mode)> {
        self.fetch_dir(dir)?;
        let dirs = self.directories.read().unwrap();
        let directory = dirs.get(&dir).unwrap(/* fetch_dir ensures it's there */);
        match directory.get(node) {
            Some((hash, mode)) => match mode.matches(filter) {
                true => Ok((*hash, *mode)),
                false => {
                    log::error!("wrong file type for {}: {:?} doesn't match {:?}", node, mode, filter);
                    Err(Error::PathError)
                },
            },
            None => Err(Error::PathError),
        }
    }

    /// Returns an iterator on the contents of a directory
    /// that was staged or commited before.
    ///
    /// Returns `PathError` if the path leads to nowhere.
    ///
    /// This can write-lock an internal RwLock for cache.
    pub fn for_each_entry<F: FnMut(&str, Mode, Hash)>(&self, path: &str, entry_type: EntryType, mut callback: F) -> Result<()> {
        let path = Path::new(path);
        let mut current = self.root.ok_or(Error::PathError)?;

        for subdir in path.all() {
            current = self.find_in_dir(current, subdir, EntryType::Directory)?.0;
        }

        self.fetch_dir(current)?;
        let dirs = self.directories.read().unwrap();
        let directory = dirs.get(&current).unwrap(/* fetch_dir ensures it's there */);
        for (node, (hash, mode)) in directory.iter() {
            if mode.matches(entry_type) {
                callback(node.as_str(), *mode, *hash);
            }
        }

        Ok(())
    }

    /// Returns the content of a file that was staged or commited before.
    ///
    /// Returns `PathError` if the path leads to nowhere.
    ///
    /// This can write-lock an internal RwLock for cache.
    pub fn read_file(&self, path: &str) -> Result<Box<[u8]>> {
        let path = Path::new(path);
        let mut current = self.root.ok_or(Error::PathError)?;

        for subdir in path.dirs()? {
            current = self.find_in_dir(current, subdir, EntryType::Directory)?.0;
        }

        let (hash, _mode) = self.find_in_dir(current, path.file()?, EntryType::File)?;
        self.any_store_get(hash, ObjectType::Blob).ok_or(Error::MissingObject)
    }

    /// Returns the content of a file that was staged or commited before.
    ///
    /// Returns `PathError` if the path leads to nowhere.
    ///
    /// This can write-lock an internal RwLock for cache.
    pub fn file_exists(&self, path: &str) -> Result<bool> {
        match self.read_file(path) {
            Ok(_) => Ok(true),
            Err(Error::PathError) => Ok(false),
            e => e.map(|_| unreachable!()),
        }
    }

    /// Returns the content of a textual file that was staged or commited before.
    ///
    /// Returns `PathError` if the path leads to nowhere.
    /// Returns `InvalidObject` if the file contains non-utf-8 bytes.
    ///
    /// This can write-lock an internal RwLock for cache.
    pub fn read_text(&self, path: &str) -> Result<String> {
        let bytes = self.read_file(path)?;
        match String::from_utf8(bytes.into_vec()) {
            Ok(string) => Ok(string),
            Err(_) => Err(Error::InvalidObject),
        }
    }

    pub(crate) fn find_committed_hash_root(&self, mut hash: Hash) -> Option<Hash> {
        while let Some(entry) = self.staged.get(hash) {
            hash = entry.delta_hint()?;
        }

        Some(hash)
    }

    pub(crate) fn update_dir<'a, I: Iterator<Item = &'a str>>(
        &mut self,
        mut directory: Directory,
        steps: &mut I,
        file_name: &str,
        data: Option<(Vec<u8>, FileType)>,
    ) -> Result<Option<Directory>> {
        let mut result = None;

        let step = steps.next();

        let node = step.unwrap_or(file_name);
        let prev_hash = directory.get(node).map(|(hash, _mode)| *hash);
        let delta_hint = prev_hash.and_then(|hash| self.find_committed_hash_root(hash));

        if step.is_some() {
            let subdir = match prev_hash {
                // no path error: use the existing dir
                Some(hash) => self.remove_dir(hash)?,
                // path error: create the dir
                None => Directory::new(),
            };

            if let Some(subdir) = self.update_dir(subdir, steps, file_name, data)? {
                let hash = serialize_directory(&mut *self.staged, &subdir, delta_hint);
                self.directories.get_mut().unwrap().insert(hash, subdir);
                result = Some((hash, Mode::Directory));
            }
        } else {
            if let Some((data, ft)) = data {
                let hash = self.staged.insert(ObjectType::Blob, data.into(), delta_hint);
                result = Some((hash, ft.into()));
            }
        }

        Ok(if let Some((hash, mode)) = result {
            if self.objects.has(hash) {
                self.staged.remove(hash);
            }

            directory.insert(node.into(), (hash, mode));
            Some(directory)
        } else {
            directory.remove(node);
            match directory.is_empty() {
                true => None,
                false => Some(directory),
            }
        })
    }

    /// Place a new file in the workspace, which will be staged
    /// until the next call to [`Self::commit`].
    ///
    /// - Missing directories are created as needed.
    /// - If `data` is `None`, any existing file at this `path`
    /// will be staged as deleted. If this leads to directories
    /// becoming empty, they will be deleted as well.
    ///
    /// Should only fail if the repository was already corrupted.
    pub fn stage(&mut self, path: &str, data: Option<(Vec<u8>, FileType)>) -> Result<()> {
        let path = Path::new(path);

        let root_dir = match self.root {
            Some(hash) => self.remove_dir(hash)?,
            None => Directory::new(),
        };

        let file_name = path.file()?;
        let mut subdirs = path.dirs()?;

        if let Some(root_dir) = self.update_dir(root_dir, &mut subdirs, file_name, data)? {
            let prev_hash = self.root.and_then(|h| self.find_committed_hash_root(h));
            let hash = serialize_directory(&mut *self.staged, &root_dir, prev_hash);
            if self.objects.has(hash) {
                self.staged.remove(hash);
            }

            self.directories.get_mut().unwrap().insert(hash, root_dir);
            self.root = Some(hash);
        } else {
            self.root = None;
        }

        Ok(())
    }

    pub(crate) fn commit_object(&mut self, hash: Hash) {
        if let Some(dir_entry) = self.staged.remove(hash) {
            if dir_entry.obj_type() == ObjectType::Tree {

                // mem::replace
                // this unwrap is questionable
                let dir = self.directories.get_mut().unwrap().insert(hash, Directory::new()).unwrap();

                #[allow(deprecated)]
                for (hash, _mode) in dir.iter_values() {
                    self.commit_object(*hash);
                }

                // mem::replace
                self.directories.get_mut().unwrap().insert(hash, dir).unwrap();
            }

            self.objects.insert(dir_entry.obj_type, dir_entry.content, Some(dir_entry.delta_hint));
        }
    }

    /// Creates a new commit which saves staged files into the
    /// repository.
    ///
    /// - If `timestamp` is `None`, the current time will be used
    /// instead.
    /// - If one of the strings in `author` & `committer` contain
    /// invalid characters (`<`, `>` or `\n`), this returns
    /// `InvalidObject` immediately.
    pub fn commit(
        &mut self,
        message: &str,
        author: (&str, &str),
        committer: (&str, &str),
        timestamp: Option<u64>,
    ) -> Result<Hash> {
        let timestamp = timestamp.unwrap_or_else(|| {
            let now = SystemTime::now();
            match now.duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                _ => 0,
            }
        });

        for string in [author.0, author.1, committer.0, committer.1] {
            let has_newline = string.contains('\n');
            let has_open = string.contains('<');
            let has_close = string.contains('>');
            if has_newline || has_open || has_close {
                return Err(Error::InvalidObject);
            }
        }

        let mut serialized = Vec::new();

        if let Some(root) = self.root {
            let head_root = if self.head.is_zero() {
                None
            } else {
                self.get_commit_root(self.head).unwrap_or(None)
            };
            
            if Some(root) != head_root {
                self.commit_object(root);
            }
        }

        let root = self.root.unwrap_or(Hash::zero());
        write!(&mut serialized, "tree {}\n", root).unwrap();

        if !self.head.is_zero() {
            write!(&mut serialized, "parent {}\n", self.head).unwrap();
        }

        write!(&mut serialized, "author {} <{}> {} +0000\n", author.0, author.1, timestamp).unwrap();
        write!(&mut serialized, "committer {} <{}> {} +0000\n", committer.0, committer.1, timestamp).unwrap();
        write!(&mut serialized, "\n{}\n", message).unwrap();

        self.head = self.objects.insert(ObjectType::Commit, serialized.into(), None);

        Ok(self.head)
    }

    /// Creates an annotated tag object and inserts it into the object store.
    pub fn create_annotated_tag(
        &mut self,
        target_commit: Hash,
        name: &str,
        message: &str,
        tagger: (&str, &str),
        timestamp: Option<u64>,
    ) -> Result<Hash> {
        let timestamp = timestamp.unwrap_or_else(|| {
            let now = SystemTime::now();
            match now.duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                _ => 0,
            }
        });

        let mut serialized = Vec::new();
        write!(&mut serialized, "object {}\n", target_commit).unwrap();
        write!(&mut serialized, "type commit\n").unwrap();
        write!(&mut serialized, "tag {}\n", name).unwrap();
        write!(&mut serialized, "tagger {} <{}> {} +0000\n", tagger.0, tagger.1, timestamp).unwrap();
        write!(&mut serialized, "\n{}\n", message).unwrap();

        Ok(self.objects.insert(ObjectType::Tag, serialized.into(), None))
    }

    /// Resets the current commit to the branch head in upstream
    ///
    /// Changes from the discarded commits are still present (staged).
    pub fn discard_commits(&mut self) {
        self.head = self.upstream_head;
    }

    /// Discard changes that weren't commited
    pub fn discard_changes(&mut self) {
        self.staged = Box::new(crate::storage::memory::MemoryStorage::new());
        self.directories.get_mut().unwrap().clear();
        self.root = self.get_commit_root(self.head).unwrap();
    }

    /// Resets the clone to the upstream state
    pub fn discard(&mut self) {
        self.discard_commits();
        self.discard_changes();
    }
}