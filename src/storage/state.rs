use std::io::Result;
use crate::internals::Hash;

/// Defines the interface for Git state management (Refs, Tags, HEAD, Index).
pub trait StateStore: Send + Sync {
    /// Read a generic reference (e.g., "refs/heads/master", "refs/tags/v1.0")
    fn read_ref(&self, name: &str) -> Option<Hash>;
    
    /// Write a generic reference
    fn write_ref(&mut self, name: &str, hash: Hash) -> Result<()>;
    
    /// Delete a reference
    fn delete_ref(&mut self, name: &str) -> Result<()>;
    
    /// List all references under a prefix (e.g., "refs/tags/")
    fn list_refs(&self, prefix: &str) -> Result<Vec<(String, Hash)>>;
    
    /// Read the HEAD pointer (returns the symbolic ref like "ref: refs/heads/master" or a detached Hash)
    fn read_head(&self) -> Option<String>;
    
    /// Write the HEAD pointer
    fn write_head(&mut self, target: &str) -> Result<()>;
    
    /// Read the staging index paths (legacy)
    fn read_index(&self) -> Option<String>;
    
    /// Read the staging index entries (paths, hashes, sizes)
    fn read_index_entries(&self) -> Option<Vec<(String, crate::internals::Hash, u32)>>;
    
    /// Write the staging index
    fn write_index(&mut self, entries: &[(String, crate::internals::Hash, u32)]) -> Result<()>;
    
    /// Clear the staging index
    fn clear_index(&mut self) -> Result<()>;

    /// Append to a reflog
    fn append_reflog(&mut self, name: &str, old_hash: Hash, new_hash: Hash, committer: &str, message: &str) -> Result<()> { Ok(()) }
    
    /// Pop the last entry from a reflog and return its old_hash (parent)
    fn pop_reflog(&mut self, name: &str) -> Result<Option<Hash>> { Ok(None) }
}
