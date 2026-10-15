use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use clap::{Parser, Subcommand};
use p2p_client::endpoint::{describe, Endpoint};
use p2p_client::exchange::exchange;
use p2p_client::punch::{punch, PunchConfig};
use p2p_client::rendezvous::Rendezvous;
use p2p_client::session::{sha256, Payload, Session, SessionConfig};
use p2p_client::{resolve_v4, stun};
use protocol::{PeerId, MAX_NAME_LEN, MAX_PAYLOAD_LEN};
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
    /// Punch a direct UDP path to the first peer under the same token, then
    /// exit. Never relays.
    Connect {
        #[command(flatten)]
        peer: PeerArgs,
    },
    /// Wait for peers under the token, one at a time, and print or save the
    /// payloads they send.
    Listen {
        #[command(flatten)]
        peer: PeerArgs,
        /// Save received files into this directory. Without it, files are
        /// only reported (size and SHA-256).
        #[arg(long)]
        save_dir: Option<PathBuf>,
    },
    /// Connect to a peer under the token, deliver one payload, and exit once
    /// the peer confirms it arrived intact.
    Send {
        #[command(flatten)]
        peer: PeerArgs,
        #[command(flatten)]
        payload: PayloadArgs,
    },
}

#[derive(clap::Args)]
struct PeerArgs {
    /// Rendezvous server (host:port), e.g. a Railway TCP proxy address.
    #[arg(long, env = "RENDEZVOUS_ADDR")]
    rendezvous: String,
    /// Share/device token. Peers with the same token find each other.
    #[arg(long, env = "P2P_TOKEN", hide_env_values = true)]
    token: String,
}

#[derive(clap::Args)]
#[group(required = true, multiple = false)]
struct PayloadArgs {
    /// Text message to send (up to 1024 bytes).
    #[arg(long)]
    message: Option<String>,
    /// Small file to send (up to 1024 bytes).
    #[arg(long)]
    file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let result = match args.command {
        Command::StunCheck => return stun_check(&args.stun).await,
        Command::Connect { peer } => connect(&peer, &args.stun).await,
        Command::Listen { peer, save_dir } => listen(&peer, &args.stun, save_dir.as_deref()).await,
        Command::Send { peer, payload } => send(&peer, &args.stun, &payload).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("{e}");
            ExitCode::FAILURE
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

/// Everything up to waiting for a peer: resolved services, our endpoint with
/// a verified STUN mapping, and a registration.
struct Setup {
    endpoint: Endpoint,
    rendezvous: Rendezvous,
    my_id: PeerId,
    stun_addrs: Vec<SocketAddr>,
}

async fn setup(peer: &PeerArgs, stun_servers: &[String]) -> Result<Setup, String> {
    let PeerArgs { rendezvous, token } = peer;
    // Each service resolves on its own, so a failure names the service.
    let rendezvous_addr = resolve_v4("rendezvous", rendezvous)
        .await
        .ok_or_else(|| format!("rendezvous {rendezvous}: cannot resolve"))?;
    let (_, stun_addrs) = resolve_stun(stun_servers).await;
    if stun_addrs.is_empty() {
        return Err("no STUN server resolved".into());
    }

    let mut endpoint = Endpoint::bind()
        .await
        .map_err(|e| format!("bind UDP endpoint: {e}"))?;
    // Detect up front so a STUN problem fails fast, before we register.
    let nat_info = endpoint
        .detect_nat(&stun_addrs, stun::Retry::DEFAULT)
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
    println!("registered as peer {my_id}");
    Ok(Setup {
        endpoint,
        rendezvous: server,
        my_id,
        stun_addrs,
    })
}

enum NextPeer {
    Connected {
        peer_id: PeerId,
        peer_addr: SocketAddr,
        rendezvous: Rendezvous,
    },
    /// Candidates were exchanged but no direct path opened. Still registered.
    PunchFailed {
        peer_id: PeerId,
        error: String,
        rendezvous: Rendezvous,
    },
}

/// Waits for the next peer under the token, trades candidates and punches.
/// Errors only if the rendezvous connection or STUN fails.
async fn next_peer(
    endpoint: &mut Endpoint,
    rendezvous: Rendezvous,
    my_id: PeerId,
    stun_addrs: &[SocketAddr],
) -> Result<NextPeer, String> {
    println!("waiting for a peer with the same token");
    let done = exchange(
        endpoint,
        rendezvous,
        my_id,
        stun_addrs,
        stun::Retry::DEFAULT,
    )
    .await
    .map_err(|e| format!("candidate exchange: {e}"))?;
    println!("candidate exchange with peer {} complete", done.peer_id);
    println!("  sent:     {}", describe(&done.my_nat));
    println!("  received: {}", describe(&done.peer_nat));

    let result = punch(
        endpoint,
        my_id,
        done.peer_id,
        &done.my_nat,
        &done.peer_nat,
        PunchConfig::DEFAULT,
    )
    .await;
    Ok(match result {
        Ok(connected) => {
            println!(
                "direct connection to peer {} at {} established in {} ms",
                done.peer_id,
                connected.peer_addr,
                connected.elapsed.as_millis()
            );
            NextPeer::Connected {
                peer_id: done.peer_id,
                peer_addr: connected.peer_addr,
                rendezvous: done.rendezvous,
            }
        }
        Err(e) => NextPeer::PunchFailed {
            peer_id: done.peer_id,
            error: e.to_string(),
            rendezvous: done.rendezvous,
        },
    })
}

async fn connect(peer: &PeerArgs, stun_servers: &[String]) -> Result<(), String> {
    let mut s = setup(peer, stun_servers).await?;
    match next_peer(&mut s.endpoint, s.rendezvous, s.my_id, &s.stun_addrs).await? {
        NextPeer::Connected { .. } => Ok(()),
        NextPeer::PunchFailed { error, .. } => Err(error),
    }
}

async fn listen(
    peer: &PeerArgs,
    stun_servers: &[String],
    save_dir: Option<&Path>,
) -> Result<(), String> {
    let Setup {
        mut endpoint,
        mut rendezvous,
        my_id,
        stun_addrs,
    } = setup(peer, stun_servers).await?;
    loop {
        let (peer_id, peer_addr, server) =
            match next_peer(&mut endpoint, rendezvous, my_id, &stun_addrs).await? {
                NextPeer::Connected {
                    peer_id,
                    peer_addr,
                    rendezvous,
                } => (peer_id, peer_addr, rendezvous),
                NextPeer::PunchFailed {
                    peer_id,
                    error,
                    rendezvous: server,
                } => {
                    log::error!("peer {peer_id}: {error}");
                    rendezvous = server;
                    continue;
                }
            };

        let mut session = Session::new(
            &mut endpoint,
            Some(server),
            my_id,
            peer_id,
            peer_addr,
            SessionConfig::DEFAULT,
        );
        loop {
            match session.recv_payload().await {
                Ok(Some(payload)) => report(peer_id, &payload, save_dir),
                Ok(None) => {
                    println!("peer {peer_id} closed the connection");
                    break;
                }
                Err(e) => {
                    println!("peer {peer_id}: {e}");
                    break;
                }
            }
        }
        rendezvous = session
            .close()
            .await
            .map_err(|e| format!("closing connection: {e}"))?
            .expect("listen sessions own the rendezvous connection");
    }
}

async fn send(
    peer: &PeerArgs,
    stun_servers: &[String],
    payload: &PayloadArgs,
) -> Result<(), String> {
    // Read and check the payload before touching the network.
    let (name, data) = load_payload(payload)?;
    let Setup {
        mut endpoint,
        rendezvous,
        my_id,
        stun_addrs,
    } = setup(peer, stun_servers).await?;
    let (peer_id, peer_addr, server) =
        match next_peer(&mut endpoint, rendezvous, my_id, &stun_addrs).await? {
            NextPeer::Connected {
                peer_id,
                peer_addr,
                rendezvous,
            } => (peer_id, peer_addr, rendezvous),
            NextPeer::PunchFailed { error, .. } => return Err(error),
        };

    let mut session = Session::new(
        &mut endpoint,
        Some(server),
        my_id,
        peer_id,
        peer_addr,
        SessionConfig::DEFAULT,
    );
    let len = data.len();
    let hash = hex(&sha256(&data));
    let started = Instant::now();
    session
        .send_payload(name, data)
        .await
        .map_err(|e| format!("peer {peer_id}: {e}"))?;
    println!(
        "delivered {len} bytes to peer {peer_id}, confirmed intact in {} ms (sha256 {hash})",
        started.elapsed().as_millis()
    );
    session
        .close()
        .await
        .map_err(|e| format!("closing connection: {e}"))?;
    Ok(())
}

/// The payload to send: `(file name, bytes)`, name unset for a message.
fn load_payload(payload: &PayloadArgs) -> Result<(Option<String>, Vec<u8>), String> {
    let (name, data) = match (&payload.message, &payload.file) {
        (Some(message), _) => (None, message.clone().into_bytes()),
        (None, Some(path)) => {
            let data = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| format!("{}: no usable file name", path.display()))?;
            if name.len() > MAX_NAME_LEN {
                return Err(format!("file name is over {MAX_NAME_LEN} bytes"));
            }
            (Some(name.to_owned()), data)
        }
        (None, None) => unreachable!("clap requires --message or --file"),
    };
    if data.len() > MAX_PAYLOAD_LEN {
        return Err(format!(
            "payload is {} bytes; the test harness sends at most {MAX_PAYLOAD_LEN}",
            data.len()
        ));
    }
    Ok((name, data))
}

fn report(peer_id: PeerId, payload: &Payload, save_dir: Option<&Path>) {
    let Some(name) = &payload.name else {
        println!(
            "peer {peer_id}: message {:?}",
            String::from_utf8_lossy(&payload.data)
        );
        return;
    };
    println!(
        "peer {peer_id}: file {name:?}, {} bytes, sha256 {}",
        payload.data.len(),
        hex(&payload.sha256)
    );
    if let Some(dir) = save_dir {
        match save(dir, name, &payload.data) {
            Ok(path) => println!("  saved to {}", path.display()),
            Err(e) => log::error!("not saved: {e}"),
        }
    }
}

/// Writes `data` to `dir/name`. The name comes from the peer, so it must be a
/// plain file name, and an existing file is never overwritten.
fn save(dir: &Path, name: &str, data: &[u8]) -> Result<PathBuf, String> {
    let plain =
        !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\0', ':']);
    if !plain {
        return Err(format!("refusing unsafe file name {name:?}"));
    }
    let path = dir.join(name);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    file.write_all(data)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(path)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
