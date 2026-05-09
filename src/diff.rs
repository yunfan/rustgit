use similar::{ChangeTag, TextDiff};

pub struct DiffHunk {
    pub header: String,
    pub lines: Vec<(char, String)>,
}

pub struct FileDiff {
    pub path: String,
    pub hunks: Vec<DiffHunk>,
}

pub fn diff_text(old_text: &str, new_text: &str, path: &str) -> FileDiff {
    let diff = TextDiff::from_lines(old_text, new_text);
    let mut hunks = Vec::new();

    for hunk in diff.unified_diff().context_radius(3).iter_hunks() {
        let header = hunk.header().to_string();
        
        let mut lines = Vec::new();
        for change in hunk.iter_changes() {
            let sign = match change.tag() {
                ChangeTag::Delete => '-',
                ChangeTag::Insert => '+',
                ChangeTag::Equal => ' ',
            };
            lines.push((sign, change.value().to_string()));
        }
        
        hunks.push(DiffHunk {
            header,
            lines,
        });
    }

    FileDiff {
        path: path.to_string(),
        hunks,
    }
}
