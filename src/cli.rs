use crate::completion::{path_completer, ZshNospace};
use crate::storage::get_file_storage;
use clap::{Args, CommandFactory, Parser, Subcommand};
use clap_complete::engine::ArgValueCompleter;
use clap_complete::env::EnvCompleter;
use clap_complete::CompleteEnv;
use std::path::{Path, PathBuf};
use url::{ParseError, Url};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.command.run()
    }

    /// Check if this is a dynamic completion request and handle it
    pub fn try_complete() -> bool {
        // Check if this is a runtime completion request (user hit TAB)
        if std::env::var("COMPLETE").is_ok() {
            // Use vanilla CompleteEnv - just return completion values
            let complete_env = CompleteEnv::with_factory(Self::command);
            complete_env.complete();
            return true;
        }
        false
    }
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Edits a remote blob and optionally stores it elsewhere.
    Edit(EditCmd),

    /// Views a remote blob.
    View(ViewCmd),

    /// Generate dynamic shell completions
    Completions(CompletionsCmd),
}

impl Commands {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Commands::Edit(cmd) => cmd.run(),
            Commands::View(cmd) => cmd.run(),
            Commands::Completions(cmd) => cmd.run(),
        }
    }
}

#[derive(Debug, Args)]
struct EditCmd {
    /// Location of the file to edit.
    #[arg(value_parser = parse_file_location)]
    source_path: Url,

    /// Final location of the edited file, if different.
    #[arg(value_parser = parse_file_location)]
    destination_path: Option<Url>,
}

impl EditCmd {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "Editing: {} -> {:?}",
            self.source_path,
            self.get_destination_path(),
        );
        Ok(())
    }

    fn get_destination_path(&self) -> &Url {
        self.destination_path.as_ref().unwrap_or(&self.source_path)
    }
}

#[derive(Debug, Args)]
struct ViewCmd {
    /// Location of the file to view.
    #[arg(value_parser = parse_file_location, add = ArgValueCompleter::new(path_completer))]
    source_path: Url,
}

impl ViewCmd {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Viewing: {}", self.source_path);
        let mut fs = get_file_storage(self.source_path.clone())?;

        // Create a buffer to read into
        let mut buf = vec![0u8; 1024]; // 1KB buffer

        // Read data from storage
        let bytes_read = fs.read(&mut buf)?;

        // Convert to string and print (assuming it's text)
        if bytes_read > 0 {
            let content = String::from_utf8_lossy(&buf[..bytes_read]);
            println!("File content ({} bytes):\n{}", bytes_read, content);
        } else {
            println!("File is empty or could not be read");
        }

        Ok(())
    }
}

#[derive(Debug, Args)]
struct CompletionsCmd {
    /// Shell type (bash, zsh, fish, etc.)
    shell: String,
}

impl CompletionsCmd {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Prepend our custom ZshNospace to default shells
        let default_shells = clap_complete::env::Shells::builtins();
        let mut custom_shells: Vec<&dyn EnvCompleter> = vec![&ZshNospace];
        custom_shells.extend(default_shells.iter());

        let shells = clap_complete::env::Shells(Box::leak(custom_shells.into_boxed_slice()));

        // Set the shell type and generate completion script
        unsafe {
            std::env::set_var("COMPLETE", self.shell.clone());
        }

        let complete_env = CompleteEnv::with_factory(Cli::command)
            .shells(shells);

        complete_env.complete();
        Ok(())
    }
}

fn parse_file_location(input: &str) -> Result<Url, String> {
    match Url::parse(input) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let absolute_path = if Path::new(input).is_absolute() {
                PathBuf::from(input)
            } else {
                std::env::current_dir()
                    .map_err(|e| format!("Can't get current directory: {}", e))?
                    .join(input)
            };
            Url::from_file_path(absolute_path).map_err(|_| "Failed to create file URL".to_string())
        }
        Err(other) => Err(other.to_string()),
    }
}
