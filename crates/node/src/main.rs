use std::future::IntoFuture;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{CommandFactory, Parser, Subcommand};
use iroh::{EndpointAddr, EndpointId};
use sync_node::folder;
use sync_node::group::GroupError;
use sync_node::key::{self, KEY_FILE};
use sync_node::lock;
use sync_node::member::{self, check_name};
use sync_node::presence::PRESENCE_INTERVAL;
use sync_node::status::STATUS_FILE;
use sync_node::ui;
use sync_node::{invite, Config, Group, Network, Node, Ticket};

/// Local-first folder sync over iroh.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Where the node keeps its key and stores. Defaults to ~/.sync-node.
    #[arg(long, env = "SYNC_NODE_CONFIG_DIR", global = true)]
    config_dir: Option<PathBuf>,

    /// Delete the config directory, after asking: the node's key (its ID),
    /// its group and its stores. Synced folders aren't touched. Runs before
    /// the command, if one is given.
    #[arg(long)]
    clear_config: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print this node's ID, creating its key if there isn't one.
    Id,
    /// Run the node until Ctrl-C, creating its key if there isn't one.
    Run {
        /// Move file contents only over direct paths, never through a relay.
        #[arg(long, env = "SYNC_NODE_DIRECT_ONLY")]
        direct_only: bool,
        /// Serve the web UI on this address, e.g. 127.0.0.1:7777. There is no
        /// auth: use a loopback address. No address, no web UI.
        #[arg(long, env = "SYNC_NODE_UI_ADDR")]
        ui_addr: Option<SocketAddr>,
        /// The built web UI. Defaults to `ui/` next to the executable.
        #[arg(long, env = "SYNC_NODE_UI_DIR")]
        ui_dir: Option<PathBuf>,
    },
    /// Found a new group with this node as its only member.
    Create {
        /// The group's name, shown with its ID on every node.
        #[arg(long)]
        name: Option<String>,
    },
    /// Manage invites to this node's group.
    Invite {
        #[command(subcommand)]
        command: InviteCommand,
    },
    /// Manage the group's synced folders.
    Folder {
        #[command(subcommand)]
        command: FolderCommand,
    },
    /// Manage the group.
    Group {
        #[command(subcommand)]
        command: GroupCommand,
    },
    /// Manage the group's members.
    Member {
        #[command(subcommand)]
        command: MemberCommand,
    },
    /// Join a group with an invite ticket, or with the node ID of a member
    /// that added this node with `member add`. That member must be running.
    Join {
        /// An invite ticket, or a member's node ID.
        ticket: Ticket,
    },
    /// Show the group as the running node sees it: members, who is online,
    /// this node's connections and traffic, and each pair's connection type.
    Status,
}

#[derive(Subcommand)]
enum InviteCommand {
    /// Print a one-time invite ticket. Works whether or not `run` is going;
    /// this node must be running for the invite to be redeemed.
    Create {
        /// How long the invite is valid, in hours.
        #[arg(long, default_value_t = 24)]
        hours: u64,
        /// The name the new member gets when it joins.
        #[arg(long)]
        name: Option<String>,
    },
}

#[derive(Subcommand)]
enum GroupCommand {
    /// Name the group, or rename it, on every node. Queued for the running
    /// node, which applies it within one presence interval.
    Name { name: String },
}

#[derive(Subcommand)]
enum FolderCommand {
    /// Keep a folder at `path` on this node: joins the group's folder of that
    /// name, or creates it. Queued for the running node, which applies it
    /// within one presence interval.
    Add {
        /// The folder's name in the group, the same on every node.
        name: String,
        /// The directory on this node.
        path: PathBuf,
    },
    /// Stop keeping a folder on this node, as before `folder add`. Its files
    /// stay on disk; only this node's own files in it (the marker, staging
    /// files) go. The group keeps the folder. Queued for the running node,
    /// which applies it within one presence interval.
    Remove { name: String },
    /// Turn a folder off on this node: it isn't watched or indexed, fetches
    /// nothing and serves nothing, and its doc still syncs. Applied by the
    /// running node within one presence interval.
    Disable { name: String },
    /// Turn a disabled folder on again. Its first scan writes what changed
    /// while it was disabled to the group, deletes included.
    Enable { name: String },
    /// Show what a folder will look like once every node keeping it has
    /// synced: files to fetch, deletes and conflicts. Written by the running
    /// node whenever the folder's index changes.
    Show {
        name: String,
        /// Also list files already in sync everywhere.
        #[arg(long)]
        all: bool,
        /// Print every node's raw index entries instead of the merged view:
        /// content hash (as `b3sum` prints it), size, modification time and
        /// version vector.
        #[arg(long, conflicts_with = "all")]
        entries: bool,
    },
}

#[derive(Subcommand)]
enum MemberCommand {
    /// Add a node to the group by its ID (`sync-node id` prints it), or rename
    /// a member. Queued for the running node, which applies it within one
    /// presence interval. The new node then joins with this node's ID.
    Add {
        id: EndpointId,
        /// Shown instead of the node ID in logs and `status`, on every node.
        #[arg(long)]
        name: Option<String>,
    },
    /// Remove a member from the group, on every node: no member connects to
    /// it or syncs with it again. Members it added go too, unless another
    /// member adds them again. A removed node can't come back with the same
    /// ID. Queued for the running node, which applies it within one presence
    /// interval.
    Remove { id: EndpointId },
}

#[tokio::main]
async fn main() -> ExitCode {
    // RUST_LOG overrides the default: our own logs at info, iroh's at warn.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,sync_node=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config_dir = match args.config_dir {
        Some(dir) => dir,
        None => default_config_dir()?,
    };
    if args.clear_config {
        clear_config(&config_dir)?;
    }
    let Some(command) = args.command else {
        if args.clear_config {
            return Ok(());
        }
        Args::command()
            .error(
                clap::error::ErrorKind::MissingSubcommand,
                "a command is required",
            )
            .exit();
    };
    let key_path = config_dir.join(KEY_FILE);
    let (key, created) = key::load_or_init(&key_path)?;
    if created {
        tracing::info!("created a new node identity at {}", key_path.display());
    }
    match command {
        Command::Id => println!("{}", key.public()),
        Command::Run {
            direct_only,
            ui_addr,
            ui_dir,
        } => {
            // Bound before the node starts, so a taken address fails `run`
            // at once.
            let listener = match ui_addr {
                Some(addr) => Some((
                    addr,
                    tokio::net::TcpListener::bind(addr)
                        .await
                        .with_context(|| format!("binding the UI address {addr}"))?,
                )),
                None => None,
            };
            let config = Config {
                direct_only,
                ..Config::new(Network::N0)
            };
            let node = Arc::new(Node::spawn_with(&config_dir, key, config).await?);
            tracing::info!("node {} running from {}", node.id(), config_dir.display());
            let (stop_ui, ui_stopped) = tokio::sync::oneshot::channel::<()>();
            let server = match listener {
                Some((addr, listener)) => {
                    let ui_dir = match ui_dir {
                        Some(dir) => dir,
                        None => ui::UiConfig::default_ui_dir()
                            .context("finding the UI dir next to the executable")?,
                    };
                    tracing::info!("web UI at http://{addr}/ from {}", ui_dir.display());
                    let (app, topics) = ui::router(
                        node.clone(),
                        ui::UiConfig {
                            addr,
                            ui_dir,
                            config_dir: config_dir.clone(),
                        },
                    );
                    let serve = axum::serve(listener, app).with_graceful_shutdown(async {
                        let _ = ui_stopped.await;
                    });
                    Some((topics, tokio::spawn(serve.into_future())))
                }
                None => None,
            };
            tokio::select! {
                _ = node.endpoint().online() => {
                    tracing::info!("online at {:?}", node.addr());
                    tokio::signal::ctrl_c().await?;
                }
                result = tokio::signal::ctrl_c() => result?,
            }
            tracing::info!("shutting down");
            if let Some((topics, server)) = server {
                // The WebSockets first: graceful shutdown waits for them.
                topics.stop().await;
                let _ = stop_ui.send(());
                server.await?.context("web UI server")?;
            }
            let node = Arc::try_unwrap(node)
                .map_err(|_| anyhow::anyhow!("the web UI still holds the node"))?;
            node.shutdown().await?;
        }
        Command::Create { name } => {
            let name = name.as_deref().map(check_name).transpose()?;
            let node = Node::spawn(&config_dir, key, Network::N0).await?;
            let result = node.create_named_group(name.as_deref()).await;
            node.shutdown().await?;
            println!("created group {}", result?);
        }
        Command::Invite {
            command: InviteCommand::Create { hours, name },
        } => {
            // Reads the group file rather than starting a node, so it works
            // while `run` holds the stores. The ticket carries only the ID:
            // the joiner finds this node's address through n0's DNS.
            let group = Group::load(&config_dir)?.ok_or(GroupError::NotInGroup)?;
            let name = name.as_deref().map(check_name).transpose()?;
            let ticket = invite::create(
                &config_dir,
                EndpointAddr::new(key.public()),
                group.id,
                Duration::from_secs(hours * 3600),
                name.as_deref(),
            )?;
            println!("{ticket}");
        }
        Command::Folder {
            command: FolderCommand::Add { name, path },
        } => {
            // Queued rather than applied, since `run` holds the stores.
            Group::load(&config_dir)?.ok_or(GroupError::NotInGroup)?;
            let name = folder::check_folder_name(&name)?;
            // As the running node would, but now rather than once it applies it.
            if let Some(kept) = folder::load_local(&config_dir)?
                .into_iter()
                .find(|f| f.name == name)
            {
                bail!(
                    "this node already keeps folder {name} at {}",
                    kept.path.display()
                );
            }
            let path = path
                .canonicalize()
                .with_context(|| format!("resolving {}", path.display()))?;
            if !path.is_dir() {
                bail!("{} is not a directory", path.display());
            }
            folder::queue_add(&config_dir, &name, &path)?;
            println!(
                "queued; the running node adds folder {name} at {} within {}s",
                path.display(),
                PRESENCE_INTERVAL.as_secs()
            );
        }
        Command::Folder {
            command: FolderCommand::Remove { name },
        } => {
            folder::queue_remove(&config_dir, &name)?;
            println!(
                "queued; the running node removes folder {name} within {}s; its files stay",
                PRESENCE_INTERVAL.as_secs()
            );
        }
        Command::Folder {
            command: FolderCommand::Disable { name },
        } => {
            folder::set_enabled(&config_dir, &name, false)?;
            println!(
                "the running node disables folder {name} within {}s",
                PRESENCE_INTERVAL.as_secs()
            );
        }
        Command::Folder {
            command: FolderCommand::Enable { name },
        } => {
            folder::set_enabled(&config_dir, &name, true)?;
            println!(
                "the running node enables folder {name} within {}s; changes made while it \
                 was disabled, deletes included, then sync to the group",
                PRESENCE_INTERVAL.as_secs()
            );
        }
        Command::Folder {
            command: FolderCommand::Show { name, all, entries },
        } => {
            let Some(view) = folder::read_view(&config_dir, &name)? else {
                bail!(
                    "no view of folder {name} yet: does this node keep it, and is `run` running?"
                );
            };
            if entries {
                print!("{}", view.render_entries());
            } else {
                print!("{}", view.render(all));
            }
            let age = sync_node::record::unix_now().saturating_sub(view.written_at);
            println!("\nas of {age}s ago");
        }
        Command::Member {
            command: MemberCommand::Add { id, name },
        } => {
            // Queued rather than written, since `run` holds the stores.
            Group::load(&config_dir)?.ok_or(GroupError::NotInGroup)?;
            let name = name.as_deref().map(check_name).transpose()?;
            member::queue_add(&config_dir, id, name.as_deref())?;
            println!(
                "queued; the running node applies it within {}s",
                PRESENCE_INTERVAL.as_secs()
            );
            if id != key.public() {
                println!(
                    "the new node joins with:\n  sync-node join {}",
                    key.public()
                );
            }
        }
        Command::Member {
            command: MemberCommand::Remove { id },
        } => {
            // Queued rather than written, since `run` holds the stores.
            let group = Group::load(&config_dir)?.ok_or(GroupError::NotInGroup)?;
            member::check_removable(id, key.public(), group.founder)?;
            member::queue_remove(&config_dir, id)?;
            println!(
                "queued; the running node applies it within {}s",
                PRESENCE_INTERVAL.as_secs()
            );
            println!(
                "members it added are no longer members, unless another member adds them again"
            );
        }
        Command::Group {
            command: GroupCommand::Name { name },
        } => {
            // Queued rather than written, since `run` holds the stores.
            Group::load(&config_dir)?.ok_or(GroupError::NotInGroup)?;
            let name = check_name(&name)?;
            member::queue_group_name(&config_dir, &name)?;
            println!(
                "queued; the running node applies it within {}s",
                PRESENCE_INTERVAL.as_secs()
            );
        }
        Command::Status => print_status(&config_dir)?,
        Command::Join { ticket } => {
            let node = Node::spawn(&config_dir, key, Network::N0).await?;
            let result = node.join(ticket).await;
            let founder = node.group().map(|group| group.founder);
            node.shutdown().await?;
            let group = result?;
            if let Some(founder) = founder {
                println!("joined group {group}, founded by {founder}; start the node with `run`");
            }
        }
    }
    Ok(())
}

/// Prints the status file the running node keeps up to date, warning if it
/// looks stale.
fn print_status(config_dir: &std::path::Path) -> anyhow::Result<()> {
    let path = config_dir.join(STATUS_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!("no status yet: is `run` running with this config dir?")
        }
        Err(err) => return Err(err.into()),
    };
    print!("{text}");
    let age = std::fs::metadata(&path)?
        .modified()?
        .elapsed()
        .unwrap_or_default();
    if age > PRESENCE_INTERVAL * 3 {
        println!(
            "\nwarning: written {}s ago; `run` doesn't seem to be running",
            age.as_secs()
        );
    }
    Ok(())
}

/// Deletes the config directory once the user confirms. Refuses a directory
/// without a key file, so a mistyped `--config-dir` can't delete something
/// else, and one a node is using (it holds the directory's lock).
fn clear_config(config_dir: &std::path::Path) -> anyhow::Result<()> {
    if !config_dir.exists() {
        println!("{} doesn't exist; nothing to clear", config_dir.display());
        return Ok(());
    }
    let key_path = config_dir.join(KEY_FILE);
    if !key_path.is_file() {
        bail!(
            "{} has no {KEY_FILE} file, so it doesn't look like a node's config dir; not deleting it",
            config_dir.display()
        );
    }
    if lock::is_held(config_dir)? {
        bail!(
            "a sync-node process (`run`, `create` or `join`) is using {}: stop it first",
            config_dir.display()
        );
    }
    let id = match key::load(&key_path) {
        Ok(key) => key.public().to_string(),
        Err(err) => format!("unreadable: {err}"),
    };
    println!(
        "This deletes {} and everything in it:",
        config_dir.display()
    );
    println!("  the node's key (ID {id}), so it gets a new ID next time");
    println!("  its group membership, invites, folder list and stores");
    println!("Synced folders aren't touched.");
    print!("Delete it? [y/N] ");
    std::io::Write::flush(&mut std::io::stdout())?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    if !matches!(answer.trim().to_lowercase().as_str(), "y" | "yes") {
        bail!("not deleted");
    }
    std::fs::remove_dir_all(config_dir)
        .with_context(|| format!("deleting {}", config_dir.display()))?;
    println!("deleted {}", config_dir.display());
    Ok(())
}

fn default_config_dir() -> anyhow::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .filter(|dir| !dir.is_empty())
        .ok_or_else(|| anyhow::anyhow!("HOME is not set: pass --config-dir"))?;
    Ok(PathBuf::from(home).join(".sync-node"))
}
