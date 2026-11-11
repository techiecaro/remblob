use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
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
    fn uri_to_path(&self, input: &str) -> PathBuf {
        // let Ok(url) = Url::parse(input) else {
        //     return PathBuf::from(input);
        // };

        let path = match input.strip_prefix("file://") {
            Some(relative) => PathBuf::from(relative),
            // If not under CWD, return full path
            _ => PathBuf::from(input),
        };

        if path.to_string_lossy().is_empty() {
            return PathBuf::from("");
        }

        path
        // let mut path = PathBuf::from(url.path());
        // if let Some(host) = url.host_str() {
        //     if !host.is_empty() {
        //         path = PathBuf::from(host).join(url.path());
        //     }
        // }
        // path
    }

    // fn is_url(&self, input: &str) -> bool {
    //     Url::parse(input).is_ok()
    // }
}

impl StorageProvider for LocalFileStorageProvider {
    fn scheme(&self) -> String {
        "file".to_owned()
    }

    fn create(&self, url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>> {
        LocalFileStorage::new(url)
    }

    fn list_paths(&self, input: &str) -> Vec<String> {
        // let is_url = crate::storage::str_to_url(input).is_ok();
        let is_url = Url::parse(input).is_ok();

        // let init_dir = PathBuf::from(input);
        // let Ok(init_dir) = uri_to_path(Url::parse(input))
        // let init_dir = self.uri_to_path(url.clone());
        let init_dir = self.uri_to_path(input);

        // Get CWD, return empty on error
        // let Ok(cwd) = std::env::current_dir() else {
        //     return vec![];
        // };

        // eprintln!("DEBUG init_dir 1 {:?}", init_dir);

        // Extract the filename prefix for filtering
        // let prefix = if init_dir.is_dir() {
        //     None
        // } else {
        //     init_dir.file_name().map(|n| n.to_string_lossy().to_string())
        // };

        eprintln!("DEBUG init_dir {:?}", init_dir);
        eprintln!(
            "DEBUG init_dir match {:?} {:?}",
            init_dir.is_dir(),
            init_dir.as_path().parent(),
        );

        let dir = match (init_dir.is_dir(), init_dir.as_path().parent()) {
            (false, Some(parent_dir)) => parent_dir.to_path_buf(),
            _ => init_dir,
        };

        eprintln!("DEBUG dir {:?}", dir);

        // if dir.to_str().is_empty()

        // Read directory, return empty on error
        // let Ok(entries) = std::fs::read_dir(&dir) else {
        //     eprintln!("DEBUG dir empty");
        //     return vec![];
        // };

        let tmp_entries = match dir.is_dir() {
            true => std::fs::read_dir(&dir),
            false => std::fs::read_dir(Path::new(".")),
        };
        let Ok(entries) = tmp_entries else {
            eprintln!("DEBUG dir empty");
            return vec![];
        };

        // let local_entries = std::fs::read_dir(Path::new(".")).unwrap();
        // let Ok(entries) = std::fs::read_dir(&dir if dir.is_dir() else Path::new(".")) else {
        //     eprintln!("DEBUG dir empty");
        //     return vec![];
        //     // local_entries
        // };

        entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let full_path = dir.join(entry.file_name());

                let path = full_path.to_string_lossy().to_string();
                // // Try to make it relative to CWD
                // let path = match full_path.strip_prefix(&cwd) {
                //     Ok(relative) => relative.to_string_lossy().to_string(),
                //     // If not under CWD, return full path
                //     _ => full_path.to_string_lossy().to_string(),
                // };

                if is_url == false {
                    // return path;
                    return Some(path);
                }

                // eprintln!("DEBUG path {:?}", path);
                let raw_url = format!("file://{}", path);
                match Url::parse(raw_url.as_str()) {
                    Ok(_) => Some(raw_url),
                    Err(_) => None,
                }
            })
            .filter(|entry| {
                // Filter by prefix - what user inputed to date
                // ZSH is doing this anyway so going to more false positives
                // ToDo: Consider moving it up a level
                entry.starts_with(input)
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
    #[case::top_level_with_dot(".", vec!["./.txt", "./1.txt", "./2.txt", "./a", "./x", "./z"], "")]
    #[case::top_level_empty_string("", vec![".txt", "1.txt", "2.txt", "a", "x", "z"], "")]
    // Partial matches at top level - show top level options
    #[case::partial_match_dot_slash_b("./b", vec![], "")]
    #[case::partial_match_b("b", vec![], "")]
    #[case::exact_filename_match("1.txt", vec!["1.txt"], "")]
    // Subdirectory paths - show contents
    #[case::subdir_a_with_dot_slash("./a", vec!["./a/a1.txt", "./a/a2.txt", "./a/b"], "")]
    #[case::subdir_a("a", vec!["a/a1.txt", "a/a2.txt", "a/b"], "")]
    #[case::nested_subdir_a_b("a/b", vec!["a/b/b1.txt", "a/b/b2.txt", "a/b/c", "a/b/d"], "")]
    #[case::nested_subdir_a_b_c("a/b/c", vec!["a/b/c/c1.txt", "a/b/c/c2.txt"], "")]
    #[case::nested_subdir_a_b_d("a/b/d", vec!["a/b/d/e"], "")]
    #[case::deeply_nested_a_b_d_e("a/b/d/e", vec!["a/b/d/e/e1.txt"], "")]
    // Non-existent paths
    #[case::nonexistent_subdir_a_x("a/x", vec![], "")]
    #[case::nonexistent_deep_path("a/x/y", vec![], "")]
    // From subdirectory a/
    #[case::from_subdir_a_current_dir(".", vec!["./a1.txt", "./a2.txt", "./b"], "a/")]
    #[case::from_subdir_a_to_b("b", vec!["b/b1.txt", "b/b2.txt", "b/c", "b/d"], "a/")]
    // From subdirectory a/b/
    #[case::from_subdir_a_b_current_dir(".", vec!["./b1.txt", "./b2.txt", "./c", "./d"], "a/b/")]
    #[case::from_subdir_a_b_to_c("c", vec!["c/c1.txt", "c/c2.txt"], "a/b/")]
    // Parent directory navigation
    #[case::parent_dir_from_a_b("..", vec!["../a1.txt", "../a2.txt", "../b"], "a/b/")]
    #[case::grandparent_dir_from_a_b("../..", vec!["../../.txt", "../../1.txt", "../../2.txt", "../../a", "../../x", "../../z"], "a/b/")]
    #[case::parent_sibling_d_from_c("../d", vec!["../d/e"], "a/b/c")]
    #[case::parent_sibling_nested_from_c("../d/e", vec!["../d/e/e1.txt"], "a/b/c")]
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

        let actual = LocalFileStorageProvider.list_paths(prefix);

        // Always restore original directory
        env::set_current_dir(original_dir).unwrap();

        assert_eq!(actual, expected);
    }

    #[rstest]
    // Top level - file:// URLs
    #[case::url_top_level_empty("file://", vec!["file://.txt", "file://1.txt", "file://2.txt", "file://a", "file://x", "file://z"], "")]
    #[case::url_top_level_dot_slash("file://./", vec!["file://./.txt", "file://./1.txt", "file://./2.txt", "file://./a", "file://./x", "file://./z"], "")]
    // Partial matches at top level
    #[case::url_partial_match_1("file://1", vec!["file://1.txt"], "")]
    #[case::url_partial_match_dot_slash_1("file://./1", vec!["file://./1.txt"], "")]
    #[case::url_no_match("file://b", vec![], "")]
    #[case::url_no_match_dot_slash("file://./b", vec![], "")]
    // Subdirectory paths
    #[case::url_subdir_a("file://a", vec!["file://a/a1.txt", "file://a/a2.txt", "file://a/b"], "")]
    #[case::url_subdir_a_slash("file://a/", vec!["file://a/a1.txt", "file://a/a2.txt", "file://a/b"], "")]
    #[case::url_subdir_dot_slash_a("file://./a", vec!["file://./a/a1.txt", "file://./a/a2.txt", "file://./a/b"], "")]
    #[case::url_nested_a_b("file://a/b", vec!["file://a/b/b1.txt", "file://a/b/b2.txt", "file://a/b/c", "file://a/b/d"], "")]
    #[case::url_nested_a_b_c("file://a/b/c", vec!["file://a/b/c/c1.txt", "file://a/b/c/c2.txt"], "")]
    // From subdirectory
    #[case::url_from_subdir_a_current("file://.", vec!["file://./a1.txt", "file://./a2.txt", "file://./b"], "a/")]
    #[case::url_from_subdir_a_to_b("file://b", vec!["file://b/b1.txt", "file://b/b2.txt", "file://b/c", "file://b/d"], "a/")]
    // Parent directory navigation
    #[case::url_parent_from_a_b("file://..", vec!["file://../a1.txt", "file://../a2.txt", "file://../b"], "a/b/")]
    #[case::url_grandparent_from_a_b("file://../..", vec!["file://../../.txt", "file://../../1.txt", "file://../../2.txt", "file://../../a", "file://../../x", "file://../../z"], "a/b/")]
    #[serial]
    fn test_list_paths_with_file_urls(
        #[case] prefix: &str,
        #[case] expected: Vec<&str>,
        #[case] cwd: &str,
        #[from(create_test_file_structure)] dir: TempDir,
    ) {
        let original_dir = env::current_dir().unwrap();

        // Change to the subdirectory specified by cwd
        env::set_current_dir(dir.path().join(cwd)).unwrap();

        println!("URL PREFIX: {prefix}");
        let actual = LocalFileStorageProvider.list_paths(prefix);

        // Always restore original directory
        env::set_current_dir(original_dir).unwrap();

        assert_eq!(actual, expected);
    }
}
