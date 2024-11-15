use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Number of worker threads
    #[arg(long, default_value_t = 1)]
    worker_nb: u16,
}
