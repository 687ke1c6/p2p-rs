use std::net::SocketAddr;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use p2p_client::endpoint::{describe, Endpoint};
use p2p_client::exchange::exchange;
use p2p_client::rendezvous::Rendezvous;
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
    /// Detect our NAT, register with the rendezvous server and trade
    /// candidates with the first peer under the same token.
    Connect {
        /// Rendezvous server (host:port), e.g. a Railway TCP proxy address.
        #[arg(long, env = "RENDEZVOUS_ADDR")]
        rendezvous: String,
        /// Share/device token. Peers with the same token find each other.
        #[arg(long, env = "P2P_TOKEN", hide_env_values = true)]
        token: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    match args.command {
        Command::StunCheck => stun_check(&args.stun).await,
        Command::Connect { rendezvous, token } => {
            match connect(&rendezvous, &token, &args.stun).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    log::error!("{e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

async fn stun_check(servers: &[String]) -> ExitCode {
    let mut socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(socket) => socket,
        Err(e) => {
            log::error!("bind UDP socket: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Ok(local) = socket.local_addr() {
        println!("local socket: {local}");
    }

    let (names, addrs) = resolve_stun(servers).await;
    let results = stun::probe(&mut socket, &addrs, stun::Retry::DEFAULT).await;
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

async fn connect(rendezvous: &str, token: &str, stun_servers: &[String]) -> Result<(), String> {
    // Each service resolves on its own, so a failure names the service.
    let rendezvous_addr = resolve_v4("rendezvous", rendezvous)
        .await
        .ok_or_else(|| format!("rendezvous {rendezvous}: cannot resolve"))?;
    let (_, stun_addrs) = resolve_stun(stun_servers).await;
    if stun_addrs.is_empty() {
        return Err("no STUN server resolved".into());
    }
    let retry = stun::Retry::DEFAULT;

    let mut endpoint = Endpoint::bind()
        .await
        .map_err(|e| format!("bind UDP endpoint: {e}"))?;
    // Detect up front so a STUN problem fails fast, before we register.
    let nat_info = endpoint
        .detect_nat(&stun_addrs, retry)
        .await
        .map_err(|e| format!("STUN: {e}"))?;
    println!("local UDP port {}", endpoint.local_port());
    println!("our candidates: {}", describe(&nat_info));

    let mut server = Rendezvous::connect(rendezvous_addr)
        .await
        .map_err(|e| format!("rendezvous {rendezvous} ({rendezvous_addr}): connect: {e}"))?;
    let my_id = server
        .register(token)
        .await
        .map_err(|e| format!("rendezvous {rendezvous}: {e}"))?;
    println!("registered as peer {my_id}; waiting for a peer with the same token");

    let done = exchange(&mut endpoint, server, my_id, &stun_addrs, retry)
        .await
        .map_err(|e| format!("candidate exchange: {e}"))?;
    println!("candidate exchange with peer {} complete", done.peer_id);
    println!("  sent:     {}", describe(&done.my_nat));
    println!("  received: {}", describe(&done.peer_nat));
    Ok(())
}

/// Resolves each STUN server on its own, skipping (and logging) failures.
async fn resolve_stun(servers: &[String]) -> (Vec<&String>, Vec<SocketAddr>) {
    let mut names = Vec::new();
    let mut addrs = Vec::new();
    for server in servers {
        if let Some(addr) = resolve_v4("STUN", server).await {
            names.push(server);
            addrs.push(addr);
        }
    }
    (names, addrs)
}
