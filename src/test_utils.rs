use std::{env, path::PathBuf};

use assert_fs::TempDir;

pub(crate) struct CwdGuard {
    pub cwd: PathBuf,
}
impl Drop for CwdGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.cwd).unwrap();
    }
}

/// Creates a temp directory containing empty files at the given relative paths.
/// Parent directories are created as needed, e.g. "a/b/c.txt" creates "a/" and "a/b/".
pub(crate) fn create_file_structure(files: &[&str]) -> TempDir {
    let dir = TempDir::new().unwrap();

    for file in files {
        let full_path = dir.join(file);
        std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        std::fs::File::create_new(full_path).unwrap();
    }

    dir
}
