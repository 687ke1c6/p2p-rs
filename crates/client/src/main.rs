use std::process::ExitCode;

use clap::{Parser, Subcommand};
use p2p_client::{resolve_v4, stun};
use tokio::net::UdpSocket;

/// Direct-only P2P client.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// STUN servers (host:port), replacing the defaults. Comma-separated in
    /// STUN_ADDRS.
    #[arg(long = "stun", env = "STUN_ADDRS", value_delimiter = ',', global = true,
          default_values = stun::DEFAULT_SERVERS)]
    stun: Vec<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Query each STUN server from one UDP socket and report the mapped
    /// addresses and the NAT type they imply.
    StunCheck,
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    match args.command {
        Command::StunCheck => stun_check(&args.stun).await,
    }
}

async fn stun_check(servers: &[String]) -> ExitCode {
    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(socket) => socket,
        Err(e) => {
            log::error!("bind UDP socket: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Ok(local) = socket.local_addr() {
        println!("local socket: {local}");
    }

    let mut names = Vec::new();
    let mut addrs = Vec::new();
    for server in servers {
        if let Some(addr) = resolve_v4("STUN", server).await {
            names.push(server);
            addrs.push(addr);
        }
    }

    let results = stun::probe(&socket, &addrs, stun::Retry::DEFAULT).await;
    let mut mappings = Vec::new();
    for ((name, addr), result) in names.iter().zip(&addrs).zip(results) {
        match result {
            Ok(mapped) => {
                println!("{name} ({addr}) -> {mapped}");
                mappings.push(mapped);
            }
            Err(e) => println!("{name} ({addr}) -> FAILED: {e}"),
        }
    }

    match stun::nat_type(&mappings) {
        Some(nat_type) => println!("NAT type: {nat_type:?}"),
        None if mappings.is_empty() => {
            println!("NAT type: unknown, no STUN server answered");
            return ExitCode::FAILURE;
        }
        None => {
            log::warn!("only one STUN server answered; assuming cone NAT");
            println!("NAT type: Cone (assumed)");
        }
    }
    ExitCode::SUCCESS
}
