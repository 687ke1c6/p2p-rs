use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{CommandFactory, Parser, Subcommand};
use iroh::{EndpointAddr, EndpointId};
use sync_node::folder;
use sync_node::group::GroupError;
use sync_node::key::{self, KEY_FILE};
use sync_node::member::{self, check_name};
use sync_node::presence::PRESENCE_INTERVAL;
use sync_node::status::STATUS_FILE;
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
    Run,
    /// Found a new group with this node as its only member.
    Create,
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
        Command::Run => {
            let config = Config::new(Network::N0);
            let node = Node::spawn_with(&config_dir, key, config).await?;
            tracing::info!("node {} running from {}", node.id(), config_dir.display());
            tokio::select! {
                _ = node.endpoint().online() => {
                    tracing::info!("online at {:?}", node.addr());
                    tokio::signal::ctrl_c().await?;
                }
                result = tokio::signal::ctrl_c() => result?,
            }
            tracing::info!("shutting down");
            node.shutdown().await?;
        }
        Command::Create => {
            let node = Node::spawn(&config_dir, key, Network::N0).await?;
            let result = node.create_group().await;
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
/// else, and one whose status file is fresh, since `run` is using it.
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
    let status_age = std::fs::metadata(config_dir.join(STATUS_FILE))
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok());
    if status_age.is_some_and(|age| age <= PRESENCE_INTERVAL * 3) {
        bail!(
            "`run` seems to be using {}: stop it first",
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
