use std::{collections::HashMap, sync::RwLock};

use once_cell::sync::Lazy;
use regex::RegexBuilder;
use std::path::{MAIN_SEPARATOR, Path, PathBuf};
use url::{ParseError, Url};

use super::local::LocalFileStorageProvider;

pub trait FileStorage: std::io::Read + std::io::Write {}

pub trait StorageProvider: Send + Sync {
    fn scheme(&self) -> String;
    fn create(&self, url: Url) -> Result<Box<dyn FileStorage>, Box<dyn std::error::Error>>;
    fn list_paths(&self, input: &str) -> Vec<String>;
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

    let url = str_to_url(input)?;
    let scheme = url.scheme();

    let registry = STORAGE_REGISTRY
        .read()
        .map_err(|_| "Storage registry unavailable")?;

    let provider = registry
        .get(scheme)
        .ok_or_else(|| format!("Scheme {} not supported", scheme))?;

    Ok(provider.list_paths(input))
}

fn to_absolute_local(path: &str) -> Result<Url, String> {
    let absolute_path = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        std::env::current_dir()
            .map_err(|e| format!("Can't get current directory: {}", e))?
            .join(path)
    };

    if absolute_path.to_string_lossy().ends_with(MAIN_SEPARATOR) {
        return Err("Path should point to a file".to_string());
    }

    Url::from_file_path(absolute_path).map_err(|_| "Failed to create file URL".to_string())
}

pub fn str_to_url(input: &str) -> Result<Url, String> {
    let file_schema_regex = RegexBuilder::new(r"^file:(?://)?(?<path>.*)")
        .case_insensitive(true)
        .build()
        .expect("File schema should be valid");

    if let Some(cap) = file_schema_regex.captures(input) {
        let file_path = cap
            .name("path")
            .expect("Should exist, or it will not match")
            .as_str();
        return to_absolute_local(file_path);
    }

    match Url::parse(input) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => to_absolute_local(input),
        Err(other) => Err(other.to_string()),
    }
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
    // Scheme only - resolves to the current directory, which is rejected (not a file)
    #[case::local_scheme_only("file://", "", "Path should point to a file")]
    // Filesystem root
    #[case::local_root_no_scheme("/", "", "Path should point to a file")]
    #[case::local_root_with_scheme("file:///", "", "Path should point to a file")]
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
    #[serial]
    fn test_str_to_url(
        #[case] input_tpl: &str,
        #[case] expected_tpl: &str,
        #[case] error: &str,
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

        match str_to_url(&input) {
            Ok(actual) => {
                assert_eq!(actual.to_string(), expected);
            }
            Err(err) => {
                assert_eq!(err, error);
            }
        }
    }
}
