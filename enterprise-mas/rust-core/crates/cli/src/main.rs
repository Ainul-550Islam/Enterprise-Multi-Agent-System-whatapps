//! `mas` — operator CLI binary. All behavior lives in [`mas_cli`]; this is
//! arg parsing in, exit code out.

use clap::Parser;

#[tokio::main]
async fn main() {
    let cli = mas_cli::Cli::parse();
    let exit = mas_cli::run(&cli.global, &cli.command).await;
    std::process::exit(exit as i32);
}
