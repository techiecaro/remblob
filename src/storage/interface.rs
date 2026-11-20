use std::{collections::HashMap, sync::RwLock};

// ToDo: Switch to std::sync::LazyLock (stable since Rust 1.80) and drop the once_cell
//       dependency. Used by STORAGE_REGISTRY and FILE_SCHEME_REGEX.
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use std::path::{MAIN_SEPARATOR, Path, PathBuf};
use url::{ParseError, Url};

use super::local::LocalFileStorageProvider;

pub trait FileStorage: std::io::Read + std::io::Write {}

pub trait StorageProvider: Send + Sync {
    fn scheme(&self) -> String;
    fn create(&self, url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>>;
    fn list_paths(&self, input: &str) -> Vec<String>;
}

#[derive(PartialEq)]
pub enum Target {
    File,
    FileOrDirectory,
}

// ToDo: Once view and edit work, verify whether RwLock (and Lazy) are still needed.
//       They were added for shell completion; the registry is never written after init.
static STORAGE_REGISTRY: Lazy<RwLock<HashMap<String, Box<dyn StorageProvider>>>> =
    Lazy::new(|| {
        let mut registry = HashMap::new();

        let providers: Vec<Box<dyn StorageProvider>> = vec![Box::new(LocalFileStorageProvider)];

        for provider in providers {
            registry.insert(provider.scheme(), provider);
        }

        RwLock::new(registry)
    });

pub fn get_file_storage(url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>> {
    Lazy::force(&STORAGE_REGISTRY);

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    let scheme = url.scheme();

    let provider = registry
        .get(scheme)
        .ok_or_else(|| format!("Scheme {} not supported", scheme))?;

    provider.create(url)
}

pub fn list_schemas() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Lazy::force(&STORAGE_REGISTRY);

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    Ok(registry.keys().map(|x| x.to_string()).collect())
}

pub fn list_paths_for_url(input: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Lazy::force(&STORAGE_REGISTRY);

    let url = str_to_url(input, Target::FileOrDirectory)?;
    let scheme = url.scheme();

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    let provider = registry
        .get(scheme)
        .ok_or_else(|| format!("Scheme {} not supported", scheme))?;

    Ok(provider.list_paths(input))
}

fn to_absolute_local(path: &str, target: Target) -> Result<Url, String> {
    let absolute_path = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        std::env::current_dir()
            .map_err(|e| format!("Can't get current directory: {}", e))?
            .join(path)
    };

    if absolute_path.to_string_lossy().ends_with(MAIN_SEPARATOR) {
        if target != Target::FileOrDirectory {
            return Err("Path should point to a file".to_string());
        }
        return Url::from_directory_path(absolute_path)
            .map_err(|_| "Failed to create directory URL".to_string());
    }

    Url::from_file_path(absolute_path).map_err(|_| "Failed to create file URL".to_string())
}

// Built once on first use; str_to_url runs per completion candidate, so rebuilding is costly.
static FILE_SCHEME_REGEX: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new(r"^file:(?://)?(?<path>.*)")
        .case_insensitive(true)
        .build()
        .expect("File schema should be valid")
});

pub fn str_to_url(input: &str, target: Target) -> Result<Url, String> {
    if let Some(cap) = FILE_SCHEME_REGEX.captures(input) {
        let file_path = cap
            .name("path")
            .expect("Should exist, or it will not match")
            .as_str();
        return to_absolute_local(file_path, target);
    }

    match Url::parse(input) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => to_absolute_local(input, target),
        Err(other) => Err(other.to_string()),
    }
}

pub fn str_to_file_url(input: &str) -> Result<Url, String> {
    str_to_url(input, Target::File)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::CwdGuard;
    use assert_fs::TempDir;
    use rstest::*;
    use serial_test::serial;
    use std::env;

    use super::*;

    #[fixture]
    fn temp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[rstest]
    #[case::local_relative_no_scheme("Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::local_absolute_no_scheme("<CWD>/Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::local_relative_with_scheme("file://Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::local_absolute_with_scheme("file://<CWD>/Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::s3_simple("s3://Bucket/Dummy.txt", "s3://Bucket/Dummy.txt", "")]
    // Nested relative paths
    #[case::local_nested_relative_no_scheme("a/b/b1.txt", "file://<CWD>/a/b/b1.txt", "")]
    #[case::local_nested_relative_with_scheme("file://a/b/b1.txt", "file://<CWD>/a/b/b1.txt", "")]
    // Dot-prefixed relative paths
    #[case::local_dot_relative_no_scheme("./a/a1.txt", "file://<CWD>/a/a1.txt", "")]
    #[case::local_dot_relative_with_scheme("file://./a/a1.txt", "file://<CWD>/a/a1.txt", "")]
    // Hidden file (name starts with a dot)
    #[case::local_hidden_no_scheme(".txt", "file://<CWD>/.txt", "")]
    #[case::local_hidden_with_scheme("file://.txt", "file://<CWD>/.txt", "")]
    // Uppercase scheme - schemes are case-insensitive
    #[case::local_uppercase_scheme_relative("FILE://Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::local_uppercase_scheme_absolute("FILE://<CWD>/Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::s3_uppercase_scheme("S3://bucket/Dummy.txt", "s3://bucket/Dummy.txt", "")]
    #[case::local_mixed_case_scheme("File://Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    // Short scheme forms - parser accepts "file:" without "//"
    #[case::local_single_slash_absolute("file:<CWD>/Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::local_no_slash_relative("file:Dummy.txt", "file://<CWD>/Dummy.txt", "")]
    #[case::local_no_slash_shorter_than_prefix("file:x", "file://<CWD>/x", "")]
    // Spaces must be percent-encoded in the URL
    #[case::local_space_no_scheme("my file.txt", "file://<CWD>/my%20file.txt", "")]
    #[case::local_space_with_scheme("file://my file.txt", "file://<CWD>/my%20file.txt", "")]
    #[case::local_space_nested_no_scheme("tmp/my file.txt", "file://<CWD>/tmp/my%20file.txt", "")]
    #[case::local_space_nested_with_scheme(
        "file://tmp/my file.txt",
        "file://<CWD>/tmp/my%20file.txt",
        ""
    )]
    // S3 nested keys keep their case and structure
    #[case::s3_nested_key("s3://bucket/Some/Deep/Key.TXT", "s3://bucket/Some/Deep/Key.TXT", "")]
    // "file:" appearing later in the input must not be treated as the scheme
    #[case::s3_key_containing_file_scheme("s3://bucket/file:x", "s3://bucket/file:x", "")]
    #[case::local_relative_containing_file_scheme("dir/file:x", "file://<CWD>/dir/file:x", "")]
    #[case::s3_with_space("s3://bucket/my file.txt", "s3://bucket/my%20file.txt", "")]
    // No trailing separator, so the target makes no difference: every case runs with both.
    #[serial]
    fn test_str_to_url(
        #[case] input_tpl: &str,
        #[case] expected_tpl: &str,
        #[case] error: &str,
        #[values(Target::File, Target::FileOrDirectory)] target: Target,
        #[from(temp_dir)] dir: TempDir,
    ) {
        let _guard = CwdGuard {
            cwd: env::current_dir().unwrap(),
        };
        let full_cwd = dir.path();

        // Run from inside the temp directory so relative paths resolve against it
        env::set_current_dir(full_cwd).unwrap();

        let input = input_tpl.replace("<CWD>", &full_cwd.to_string_lossy());
        let expected = expected_tpl.replace("<CWD>", &full_cwd.to_string_lossy());

        match str_to_url(&input, target) {
            Ok(actual) => {
                assert_eq!(actual.to_string(), expected);
            }
            Err(err) => {
                assert_eq!(err, error);
            }
        }
    }

    // Inputs ending with a separator: rejected for Target::File,
    // turned into a directory URL (with trailing "/") for Target::FileOrDirectory.
    #[rstest]
    // Scheme only - resolves to the current directory
    #[case::scheme_only_file("file://", Target::File, "", "Path should point to a file")]
    #[case::scheme_only_dir("file://", Target::FileOrDirectory, "file://<CWD>/", "")]
    // Filesystem root
    #[case::root_no_scheme_file("/", Target::File, "", "Path should point to a file")]
    #[case::root_no_scheme_dir("/", Target::FileOrDirectory, "file:///", "")]
    #[case::root_with_scheme_file("file:///", Target::File, "", "Path should point to a file")]
    #[case::root_with_scheme_dir("file:///", Target::FileOrDirectory, "file:///", "")]
    // Relative directory
    #[case::relative_no_scheme_file("a/", Target::File, "", "Path should point to a file")]
    #[case::relative_no_scheme_dir("a/", Target::FileOrDirectory, "file://<CWD>/a/", "")]
    #[case::relative_with_scheme_file("file://a/", Target::File, "", "Path should point to a file")]
    #[case::relative_with_scheme_dir("file://a/", Target::FileOrDirectory, "file://<CWD>/a/", "")]
    #[case::relative_uppercase_scheme_dir("FILE://a/", Target::FileOrDirectory, "file://<CWD>/a/", "")]
    #[case::relative_nested_dir("file://a/b/", Target::FileOrDirectory, "file://<CWD>/a/b/", "")]
    #[case::relative_dot_dir("./", Target::FileOrDirectory, "file://<CWD>/", "")]
    // Absolute directory
    #[case::absolute_no_scheme_dir("<CWD>/a/", Target::FileOrDirectory, "file://<CWD>/a/", "")]
    #[case::absolute_single_slash_dir("file:<CWD>/a/", Target::FileOrDirectory, "file://<CWD>/a/", "")]
    #[case::absolute_with_scheme_dir("file://<CWD>/a/", Target::FileOrDirectory, "file://<CWD>/a/", "")]
    // Directory name that needs escaping
    #[case::space_dir("file://my dir/", Target::FileOrDirectory, "file://<CWD>/my%20dir/", "")]
    // The check is on the trailing separator, not the filesystem:
    // "a" without "/" is treated as a file path for both targets.
    #[case::no_trailing_separator_file("file://a", Target::File, "file://<CWD>/a", "")]
    #[case::no_trailing_separator_dir("file://a", Target::FileOrDirectory, "file://<CWD>/a", "")]
    // Non-file schemes are passed through untouched, trailing "/" included
    #[case::s3_prefix_file("s3://bucket/dir/", Target::File, "s3://bucket/dir/", "")]
    #[case::s3_prefix_dir("s3://bucket/dir/", Target::FileOrDirectory, "s3://bucket/dir/", "")]
    #[serial]
    fn test_str_to_url_trailing_separator(
        #[case] input_tpl: &str,
        #[case] target: Target,
        #[case] expected_tpl: &str,
        #[case] error: &str,
        #[from(temp_dir)] dir: TempDir,
    ) {
        let _guard = CwdGuard {
            cwd: env::current_dir().unwrap(),
        };
        let full_cwd = dir.path();
        env::set_current_dir(full_cwd).unwrap();

        let input = input_tpl.replace("<CWD>", &full_cwd.to_string_lossy());
        let expected = expected_tpl.replace("<CWD>", &full_cwd.to_string_lossy());

        match str_to_url(&input, target) {
            Ok(actual) => {
                assert_eq!(actual.to_string(), expected);
            }
            Err(err) => {
                assert_eq!(err, error);
            }
        }
    }

    // Full flow used by `view`: user input -> str_to_url -> storage -> read.
    // `file_on_disk` is relative to the temp dir; `input_tpl` is what the user types.
    #[rstest]
    #[case::relative_space("my file.txt", "my file.txt")]
    #[case::scheme_space("file://my file.txt", "my file.txt")]
    #[case::uppercase_scheme_space("FILE://my file.txt", "my file.txt")]
    #[case::single_slash_absolute_space("file:<CWD>/my file.txt", "my file.txt")]
    #[case::scheme_absolute_space("file://<CWD>/my file.txt", "my file.txt")]
    #[case::nested_space("sub dir/my file.txt", "sub dir/my file.txt")]
    #[case::scheme_nested_space("file://sub dir/my file.txt", "sub dir/my file.txt")]
    #[case::relative_percent("50%.txt", "50%.txt")]
    #[case::scheme_percent("file://50%.txt", "50%.txt")]
    #[case::scheme_percent_escape_lookalike("file://%41.txt", "%41.txt")]
    #[case::scheme_hash("file://a#b.txt", "a#b.txt")]
    #[case::scheme_question_mark("file://q?x.txt", "q?x.txt")]
    #[case::scheme_uppercase_name("file://Dummy.txt", "Dummy.txt")]
    #[case::scheme_non_ascii("file://Zażółć.txt", "Zażółć.txt")]
    // Reading a file must work the same whichever target was used to parse it.
    #[serial]
    fn test_read_through_str_to_url(
        #[case] input_tpl: &str,
        #[case] file_on_disk: &str,
        #[values(Target::File, Target::FileOrDirectory)] target: Target,
        #[from(temp_dir)] dir: TempDir,
    ) {
        let _guard = CwdGuard {
            cwd: env::current_dir().unwrap(),
        };
        let full_cwd = dir.path();
        env::set_current_dir(full_cwd).unwrap();

        let content = "test content";
        let disk_path = full_cwd.join(file_on_disk);
        std::fs::create_dir_all(disk_path.parent().unwrap()).unwrap();
        std::fs::write(&disk_path, content).unwrap();

        let input = input_tpl.replace("<CWD>", &full_cwd.to_string_lossy());
        let url = str_to_url(&input, target).unwrap();
        let mut storage = get_file_storage(url).unwrap();

        let mut actual = String::new();
        storage.read_to_string(&mut actual).unwrap();

        assert_eq!(actual, content);
    }

    // A directory URL is accepted by str_to_url (FileOrDirectory) and by storage creation,
    // but reading it fails with IsADirectory. Documents current behaviour for `view`.
    #[rstest]
    #[case::relative_no_scheme("a/", "a")]
    #[case::relative_with_scheme("file://a/", "a")]
    #[case::uppercase_scheme("FILE://a/", "a")]
    #[case::absolute_with_scheme("file://<CWD>/a/", "a")]
    #[case::space("file://my dir/", "my dir")]
    #[case::current_dir("file://", "")]
    #[serial]
    fn test_read_through_str_to_url_directory(
        #[case] input_tpl: &str,
        #[case] dir_on_disk: &str,
        #[from(temp_dir)] dir: TempDir,
    ) {
        let _guard = CwdGuard {
            cwd: env::current_dir().unwrap(),
        };
        let full_cwd = dir.path();
        env::set_current_dir(full_cwd).unwrap();

        std::fs::create_dir_all(full_cwd.join(dir_on_disk)).unwrap();

        let input = input_tpl.replace("<CWD>", &full_cwd.to_string_lossy());
        let url = str_to_url(&input, Target::FileOrDirectory).unwrap();
        let mut storage = get_file_storage(url).unwrap();

        let result = storage.read_to_string(&mut String::new());

        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::IsADirectory
        );
    }
}
