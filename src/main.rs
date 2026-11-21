use mini_redis::config::{load, HELP_TEXT};
use mini_redis::server;

fn main() {
    let parsed = match load() {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("configuration error: {}", error);
            eprintln!("Run with --help for usage.");
            std::process::exit(2);
        }
    };

    if parsed.help {
        print!("{HELP_TEXT}");
        return;
    }

    if let Err(error) = server::run(parsed.config) {
        eprintln!("failed to start: {error}");
        std::process::exit(1);
    }
}
