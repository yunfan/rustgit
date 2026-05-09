use ignore::gitignore::GitignoreBuilder;
use std::path::Path;

pub struct IgnoreManager {
    gitignore: ignore::gitignore::Gitignore,
}

impl IgnoreManager {
    pub fn new(repo_path: &str) -> Self {
        let mut builder = GitignoreBuilder::new(repo_path);
        let gitignore_path = Path::new(repo_path).join(".gitignore");
        if gitignore_path.exists() {
            if let Some(e) = builder.add(gitignore_path) {
                log::warn!("Failed to add .gitignore: {}", e);
            }
        }
        let gitignore = builder.build().unwrap_or_else(|_| GitignoreBuilder::new(repo_path).build().unwrap());
        Self { gitignore }
    }

    pub fn is_ignored(&self, path: &str, is_dir: bool) -> bool {
        if path.starts_with(".git/") || path == ".git" {
            return true;
        }
        self.gitignore.matched_path_or_any_parents(path, is_dir).is_ignore()
    }
}
