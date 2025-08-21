use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
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
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Edits a remote blob and optionally stores it elsewhere.
    Edit(EditCmd),

    /// Views a remote blob.
    View(ViewCmd),
}

impl Commands {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Commands::Edit(cmd) => cmd.run(),
            Commands::View(cmd) => cmd.run(),
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
    #[arg(value_parser = parse_file_location)]
    source_path: Url,
}

impl ViewCmd {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Viewing: {}", self.source_path);
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
