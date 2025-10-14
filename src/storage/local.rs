use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

use itertools::Itertools;
use url::Url;

use super::interface::{FileStorage, StorageProvider};

struct LocalFileStorage {
    url: Url,
    file: Option<File>,
    mode: FileMode,
}

#[derive(PartialEq)]
enum FileMode {
    None,
    Reading,
    Writing,
}

impl LocalFileStorage {
    fn ensure_read_mode(&mut self) -> std::io::Result<&mut File> {
        if self.mode != FileMode::Reading {
            self.file = Some(OpenOptions::new().read(true).open(self.url.path())?);
            self.mode = FileMode::Reading;
        }
        Ok(self.file.as_mut().unwrap())
    }

    fn ensure_write_mode(&mut self) -> std::io::Result<&mut File> {
        if self.mode != FileMode::Writing {
            self.file = Some(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(self.url.path())?,
            );
            self.mode = FileMode::Writing;
        }

        Ok(self.file.as_mut().unwrap())
    }

    fn new(url: url::Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>> {
        let storage = LocalFileStorage {
            url,
            file: None,
            mode: FileMode::None,
        };
        Ok(Box::new(storage))
    }
}

impl FileStorage for LocalFileStorage {}

impl std::io::Read for LocalFileStorage {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let file = self.ensure_read_mode()?;
        file.read(buf)
    }
}

impl std::io::Write for LocalFileStorage {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let file = self.ensure_write_mode()?;
        file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let file = self.ensure_write_mode()?;
        file.flush()
    }
}

pub struct LocalFileStorageProvider;

impl LocalFileStorageProvider {
    fn uri_to_path(&self, url: Url) -> PathBuf {
        let mut path = PathBuf::from(url.path());
        if let Some(host) = url.host_str() {
            if !host.is_empty() {
                path = PathBuf::from(host).join(url.path());
            }
        }
        path
    }
}

impl StorageProvider for LocalFileStorageProvider {
    fn scheme(&self) -> String {
        "file".to_owned()
    }

    fn create(&self, url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>> {
        LocalFileStorage::new(url)
    }

    fn list_paths(&self, url: Url) -> Vec<String> {
        let init_dir = self.uri_to_path(url);

        // Get CWD, return empty on error
        let Ok(cwd) = std::env::current_dir() else {
            return vec![];
        };

        let dir = match (init_dir.is_dir(), init_dir.as_path().parent()) {
            (false, Some(parent_dir)) => parent_dir.to_path_buf(),
            _ => init_dir,
        };

        // Read directory, return empty on error
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return vec![];
        };

        entries
            .filter_map(|entry| entry.ok())
            .map(|entry| {
                let full_path = dir.join(entry.file_name());

                // Try to make it relative to CWD
                if let Ok(relative) = full_path.strip_prefix(&cwd) {
                    relative.to_string_lossy().to_string()
                } else {
                    // If not under CWD, return full path
                    full_path.to_string_lossy().to_string()
                }
            })
            .sorted()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use assert_fs::assert::PathAssert;
    use assert_fs::fixture::FileWriteStr;
    use assert_fs::{NamedTempFile, TempDir};
    use pretty_assertions::assert_eq;
    use rstest::*;
    use serial_test::serial;

    use super::*;

    #[test]
    fn test_read_success() {
        let content = "test content";
        let tempfile = NamedTempFile::new("foo.txt").unwrap();
        tempfile.write_str(content).unwrap();
        let url = Url::from_file_path(tempfile.path()).unwrap();

        let mut storage = LocalFileStorageProvider.create(url).unwrap();

        let mut actual = String::new();
        storage.read_to_string(&mut actual).unwrap();

        assert_eq!(content, actual);
    }

    #[test]
    fn test_read_nonexistent_file() {
        let url = Url::parse("file:///this/file/does-not-exist.txt").unwrap();

        let mut storage = LocalFileStorageProvider.create(url).unwrap();

        let result = storage.read_to_string(&mut String::new());

        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_write_success_file_does_not_exist() {
        let content = "test content";
        let tempfile = NamedTempFile::new("foo.txt").unwrap();
        tempfile.assert(predicates::path::missing());

        let url = Url::from_file_path(tempfile.path()).unwrap();

        let mut storage = LocalFileStorageProvider.create(url).unwrap();

        storage.write_all(content.as_bytes()).unwrap();

        tempfile.assert(predicates::path::exists());
        tempfile.assert(content);
    }

    #[test]
    fn test_write_success_overwrite() {
        let initial_content = "initial content";
        let new_content = "new content";
        let tempfile = NamedTempFile::new("foo.txt").unwrap();
        tempfile.write_str(initial_content).unwrap();
        tempfile.assert(initial_content);

        let url = Url::from_file_path(tempfile.path()).unwrap();

        let mut storage = LocalFileStorageProvider.create(url).unwrap();
        storage.write_all(new_content.as_bytes()).unwrap();

        tempfile.assert(new_content);
    }

    #[test]
    fn test_write_permission_denied() {
        let tempfile = NamedTempFile::new("readonly.txt").unwrap();
        tempfile.write_str("initial content").unwrap();

        #[cfg(unix)]
        {
            use std::fs;
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(tempfile.path()).unwrap().permissions();
            perms.set_mode(0o444); // read-only
            fs::set_permissions(tempfile.path(), perms).unwrap();

            let url = Url::from_file_path(tempfile.path()).unwrap();
            let mut storage = LocalFileStorageProvider.create(url).unwrap();

            let result = storage.write_all(b"new content");
            assert_eq!(
                result.unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        }
    }

    const FILES: &[&str] = &[
        "1.txt",
        "2.txt",
        ".txt",
        "a/a1.txt",
        "a/a2.txt",
        "a/b/b1.txt",
        "a/b/b2.txt",
        "a/b/c/c1.txt",
        "a/b/c/c2.txt",
        "a/b/d/e/e1.txt",
        "x",
        "z",
    ];

    #[fixture]
    fn create_test_file_structure() -> TempDir {
        let dir = TempDir::new().unwrap();

        for file in FILES {
            let full_path = dir.join(file);
            std::fs::create_dir_all(full_path.as_path().parent().unwrap()).unwrap();
            std::fs::File::create_new(full_path).unwrap();
        }

        dir
    }

    #[rstest]
    // Top level - current directory
    #[case(".", vec![".txt", "1.txt", "2.txt", "a", "x", "z"], "")]
    #[case("", vec![".txt", "1.txt", "2.txt", "a", "x", "z"], "")]
    // Partial matches at top level - show top level options
    #[case("./b", vec![".txt", "1.txt", "2.txt", "a", "x", "z"], "")]
    #[case("b", vec![".txt", "1.txt", "2.txt", "a", "x", "z"], "")]
    #[case("1.txt", vec![".txt", "1.txt", "2.txt", "a", "x", "z"], "")]
    // Subdirectory paths - show contents
    #[case("./a", vec!["a/a1.txt", "a/a2.txt", "a/b"], "")]
    #[case("a", vec!["a/a1.txt", "a/a2.txt", "a/b"], "")]
    #[case("a/b", vec!["a/b/b1.txt", "a/b/b2.txt", "a/b/c", "a/b/d"], "")]
    #[case("a/b/c", vec!["a/b/c/c1.txt", "a/b/c/c2.txt"], "")]
    #[case("a/b/d", vec!["a/b/d/e"], "")]
    #[case("a/b/d/e", vec!["a/b/d/e/e1.txt"], "")]
    // Non-existent paths
    #[case("a/x", vec!["a/a1.txt", "a/a2.txt", "a/b"], "")]
    #[case("a/x/y", vec![], "")]
    // From subdirectory a/
    #[case(".", vec!["a1.txt", "a2.txt", "b"], "a/")]
    #[case("b", vec!["b/b1.txt", "b/b2.txt", "b/c", "b/d"], "a/")]
    // From subdirectory a/b/
    #[case(".", vec!["b1.txt", "b2.txt", "c", "d"], "a/b/")]
    #[case("c", vec!["c/c1.txt", "c/c2.txt"], "a/b/")]
    // Parent directory navigation
    #[case("..", vec!["../a1.txt", "../a2.txt", "../b"], "a/b/")]
    #[case("../..", vec!["../../.txt", "../../1.txt", "../../2.txt", "../../a", "../../x", "../../z"], "a/b/")]
    #[case("../d", vec!["../d/e"], "a/b/c")]
    #[case("../d/e", vec!["../d/e/e1.txt"], "a/b/c")]
    #[serial]
    fn test_list_paths(
        #[case] prefix: &str,
        #[case] expected: Vec<&str>,
        #[case] cwd: &str,
        #[from(create_test_file_structure)] dir: TempDir,
    ) {
        let original_dir = env::current_dir().unwrap();

        // Change to the subdirectory specified by cwd
        env::set_current_dir(dir.path().join(cwd)).unwrap();

        let url = url::Url::from_file_path(dir.path().join(cwd).join(prefix).as_path()).unwrap();

        let actual = LocalFileStorageProvider.list_paths(url);

        // Always restore original directory
        env::set_current_dir(original_dir).unwrap();

        assert_eq!(actual, expected);
    }
}
