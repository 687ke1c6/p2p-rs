use std::net::SocketAddr;

use clap::Parser;
use tokio::net::TcpListener;

const DEFAULT_PORT: u16 = 7000;

/// Rendezvous server: passes hole-punch candidates between peers that share a
/// token. Carries no file data.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Address to listen on [default: 0.0.0.0:$PORT, or 0.0.0.0:7000 if PORT is unset]
    #[arg(long, env = "LISTEN_ADDR")]
    listen: Option<SocketAddr>,

    /// Port to listen on when --listen is not given. Railway sets this.
    #[arg(long, env = "PORT", default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Log to stdout: hosts like Railway label every stderr line as an error.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Stdout)
        .init();
    let args = Args::parse();
    let addr = args
        .listen
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], args.port)));

    let listener = TcpListener::bind(addr).await?;
    log::info!("listening on tcp://{}", listener.local_addr()?);

    tokio::select! {
        result = rendezvous_server::serve(listener) => result,
        _ = shutdown_signal() => {
            log::info!("shutting down");
            Ok(())
        }
    }
}

/// Resolves on Ctrl-C or SIGTERM. Containers stop with SIGTERM, and a PID 1
/// that ignores it is killed only after a grace period.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
