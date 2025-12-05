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

    let mut completions: Vec<CompletionCandidate> = crate::storage::list_schemas()
        .unwrap_or_default()
        .iter()
        .map(|s| CompletionCandidate::new(format!("{s}://")).hide(false))
        .collect();

    completions.push(CompletionCandidate::new("./").hide(false));
    completions.push(CompletionCandidate::new("testA").hide(false));
    completions.push(CompletionCandidate::new("testB").hide(false));

    if current_str.is_empty() {
        return completions;
    }

    completions
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
    local _CLAP_COMPLETE_INDEX=$(expr $CURRENT - 1)
    local _CLAP_IFS=$'\n'

    local completions=("${(@f)$( \
        _CLAP_IFS="$_CLAP_IFS" \
        _CLAP_COMPLETE_INDEX="$_CLAP_COMPLETE_INDEX" \
        VAR="zsh" \
        COMPLETER -- "${words[@]}" 2>/dev/null \
    )}")

    if [[ -n $completions ]]; then
        # Smart nospace handling for path-like completions
        local has_suffix=false
        for comp in $completions; do
            if [[ "$comp" =~ '[=/:]$' ]]; then
                has_suffix=true
                break
            fi
        done

        if [[ $has_suffix == true ]]; then
            # Manually add completions without trailing space
            for comp in $completions; do
                # Remove escaped colons temporarily to check for description separator
                local temp="${comp//\\:}"
                if [[ "$temp" == *:* ]]; then
                    # Has unescaped colon (description): split on it
                    local value="${comp%%:*}"
                    value="${value//\\:/:}"
                else
                    # No description: just unescape
                    local value="${comp//\\:/:}"
                fi
                compadd -S '' -- "$value"
            done
        else
            _describe 'values' completions
        fi
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
        println!("AAAA?");
        Ok(())
    }
}
