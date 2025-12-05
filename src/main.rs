mod cli;
mod completion;
mod storage;

use clap::Parser;
use cli::Cli;

fn main() {
    // Handle dynamic completion requests BEFORE parsing
    if Cli::try_complete() {
        return;
    }

    let cli = Cli::parse();

    if let Err(e) = cli.run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
