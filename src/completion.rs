//! Shell completion support for remblob
//!
//! This module provides:
//! - Custom shell completers (ZSH with nospace support)
//! - Dynamic path completion for storage URIs

use clap_complete::engine::CompletionCandidate;
use clap_complete::env::EnvCompleter;
use std::io::Write;

/// Custom ZSH completer with nospace support for path-like completions
///
/// This completer extends the default ZSH completion by detecting when
/// completions end with path-like suffixes (/, :, =) and preventing
/// the shell from adding a trailing space after completion.
pub struct ZshNospace;

/// Provides dynamic path completion for file/URL arguments
///
/// Returns completion candidates based on the current input:
/// - **Empty input**: Shows available storage schemes (s3://, file://) and local path (./)
/// - **Non-empty input**: Currently returns schemes (will be enhanced to call `list_paths()`)
///
/// # Example completions
/// - `""` → `["s3://", "file://", "./"]`
/// - `"s3://"` → `["s3://", "file://", "./"]` (TODO: list S3 buckets)
/// - `"./src"` → `["s3://", "file://", "./"]` (TODO: list files in ./src)
pub fn path_completer(current: &std::ffi::OsStr) -> Vec<CompletionCandidate> {
    let Some(current_str) = current.to_str() else {
        return vec![];
    };

    let schemes: Vec<CompletionCandidate> = crate::storage::list_schemas()
        .unwrap_or_default()
        .iter()
        .map(|s| CompletionCandidate::new(format!("{s}://")).hide(false))
        .collect();

    eprintln!("DEBUG str {:?}", current_str);

    let Ok(paths) = crate::storage::list_paths_for_url(current_str) else {
        return schemes;
    };

    eprintln!("DEBUG paths {}", paths.join(";"));

    paths
        .iter()
        .map(|s| CompletionCandidate::new(s).hide(false))
        .chain(schemes)
        .collect()
}

impl EnvCompleter for ZshNospace {
    fn name(&self) -> &'static str {
        "zsh"
    }

    fn is(&self, name: &str) -> bool {
        name == "zsh"
    }

    fn write_registration(
        &self,
        var: &str,
        name: &str,
        bin: &str,
        completer: &str,
        buf: &mut dyn Write,
    ) -> Result<(), std::io::Error> {
        let escaped_name = name.replace('-', "_");
        // Simple quoting - wrap in quotes if contains spaces
        let bin = if bin.contains(' ') {
            format!("\"{}\"", bin)
        } else {
            bin.to_string()
        };
        let completer = if completer.contains(' ') {
            format!("\"{}\"", completer)
        } else {
            completer.to_string()
        };

        // Modified ZSH script with nospace handling
        let script = r#"#compdef BIN
function _clap_dynamic_completer_NAME() {
    local completions=("${(@f)$(
        VAR=zsh \
        _CLAP_COMPLETE_INDEX=$((CURRENT - 1)) \
        COMPLETER -- "${words[@]}" 2>/dev/null
    )}")

    [[ -z $completions ]] && return

    local -a values=() descs=()

    for comp in $completions; do
        local temp="${comp//\\:}"
        if [[ "$temp" == *:* ]]; then
            local value="${comp%%:*}"
            local desc="${comp#*:}"
            value="${value//\\:/:}"
            values+=("$value")
            descs+=("$value  ($desc)")
        else
            local value="${comp//\\:/:}"
            values+=("$value")
            descs+=("$value")
        fi
    done

    # Check if any value needs no-space
    local needs_nosuffix=false
    for v in $values; do
        [[ "$v" == *[=/] ]] && needs_nosuffix=true && break
    done

    if [[ $needs_nosuffix == true ]]; then
        compadd -l -d descs -S '' -- "${values[@]}"
    else
        _describe 'values' completions
    fi
}

compdef _clap_dynamic_completer_NAME BIN"#
            .replace("NAME", &escaped_name)
            .replace("COMPLETER", &completer)
            .replace("BIN", &bin)
            .replace("VAR", var);

        write!(buf, "{}", script)
    }

    fn write_complete(
        &self,
        _cmd: &mut clap::Command,
        _args: Vec<std::ffi::OsString>,
        _current_dir: Option<&std::path::Path>,
        _buf: &mut dyn Write,
    ) -> Result<(), std::io::Error> {
        // Delegate to default implementation
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{CwdGuard, create_file_structure};
    use assert_fs::TempDir;
    use rstest::*;
    use serial_test::serial;
    use std::env;
    use std::ffi::OsStr;

    use super::*;

    const FILES: &[&str] = &[
        "1.txt",
        "2.txt",
        ".txt",
        "my file.txt",
        "myfile.txt",
        "a/a1.txt",
        "a/a2.txt",
        "a/my file.txt",
        "a/myfile.txt",
        "a/b/b1.txt",
        "a/b/b2.txt",
        "a/b/c/c1.txt",
        "a/b/c/c2.txt",
        "a/b/d/e/e1.txt",
        "x",
        "z",
        "dir/d1.txt",
        "dir/d2.txt",
        "Dummy.txt",
        "Dummy1.txt",
        "Dummy2.txt",
    ];

    #[fixture]
    fn create_test_file_structure() -> TempDir {
        create_file_structure(FILES)
    }

    // Candidates come back sorted by byte value: '.' < digits < uppercase < lowercase,
    // and a space sorts before letters ("my file.txt" < "myfile.txt").
    // The scheme list ("file://") is always appended at the end.
    #[rstest]
    // Empty input / bare scheme - list the current directory
    #[case::local_empty("", vec![".txt", "1.txt", "2.txt", "Dummy.txt", "Dummy1.txt", "Dummy2.txt", "a", "dir", "my file.txt", "myfile.txt", "x", "z", "file://"])]
    #[case::local_file_empty("file://", vec!["file://.txt", "file://1.txt", "file://2.txt", "file://Dummy.txt", "file://Dummy1.txt", "file://Dummy2.txt", "file://a", "file://dir", "file://my file.txt", "file://myfile.txt", "file://x", "file://z", "file://"])]
    #[case::local_m("m", vec!["my file.txt", "myfile.txt", "file://"])]
    #[case::local_file_m("file://m", vec!["file://my file.txt", "file://myfile.txt", "file://"])]
    #[case::local_file_a("file://a", vec!["file://a/a1.txt", "file://a/a2.txt", "file://a/b", "file://a/my file.txt", "file://a/myfile.txt", "file://"])]
    // Directory with trailing separator - list its contents
    #[case::local_dir_slash("a/", vec!["a/a1.txt", "a/a2.txt", "a/b", "a/my file.txt", "a/myfile.txt", "file://"])]
    #[case::local_file_dir_slash("file://a/", vec!["file://a/a1.txt", "file://a/a2.txt", "file://a/b", "file://a/my file.txt", "file://a/myfile.txt", "file://"])]
    #[case::local_file_d("file://d", vec!["file://dir", "file://"])]
    #[case::local_file_cap_d("file://D", vec!["file://Dummy.txt", "file://Dummy1.txt", "file://Dummy2.txt", "file://"])]
    #[serial]
    fn test_path_completer(
        #[case] input: &str,
        #[case] expected: Vec<&str>,
        #[from(create_test_file_structure)] dir: TempDir,
    ) {
        let _guard = CwdGuard {
            cwd: env::current_dir().unwrap(),
        };

        // Run from inside the temp directory so relative paths resolve against it
        env::set_current_dir(dir.path()).unwrap();

        let candidates = path_completer(OsStr::new(input));
        let candidates_values: Vec<String> = candidates
            .iter()
            .map(|c| c.get_value().to_str().unwrap().to_string())
            .collect();

        assert_eq!(candidates_values, expected);
    }
}
