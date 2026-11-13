use std::{env, path::PathBuf};

pub(crate) struct CwdGuard {
    pub cwd: PathBuf,
}
impl Drop for CwdGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.cwd).unwrap();
    }
}
