//! The web UI's actions: the setup the CLI does with `create`, `join`,
//! `invite create`, `member add`, `member remove`, `group name` and
//! `folder add`, with the same checks and error texts.
//!
//! Create and join run as tasks that finish even if their request goes (a
//! closed tab, the join limit): stopped half-way, they could leave the node
//! in a group without its monitor. The others go through the queues the
//! running node applies, as from the CLI.

use std::path::PathBuf;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use iroh::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use super::{ApiError, AppState, UiBackend};
use crate::group::GroupError;
use crate::member::{check_name, check_removable};
use crate::presence::PRESENCE_INTERVAL;
use crate::record::GroupId;
use crate::{folder, invite, member, Ticket};

/// How long `POST /api/join` waits for the join. It goes on after that.
pub const JOIN_LIMIT: Duration = Duration::from_secs(60);

/// `POST /api/group`.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct CreateRequest {
    /// The group's name, shown with its ID on every node.
    #[serde(default)]
    #[cfg_attr(test, ts(optional))]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct CreateAnswer {
    pub group: String,
}

/// `POST /api/join`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct JoinRequest {
    /// An invite ticket, or the node ID of a member that added this node.
    pub ticket: String,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct JoinAnswer {
    pub group: String,
    pub founder: String,
}

/// `POST /api/invites`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct InviteRequest {
    /// How long the invite is valid, in hours. 24 if left out.
    #[serde(default)]
    #[cfg_attr(test, ts(optional))]
    pub hours: Option<u64>,
    /// The name the new member gets when it joins.
    #[serde(default)]
    #[cfg_attr(test, ts(optional))]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct InviteAnswer {
    pub ticket: String,
}

/// `POST /api/members`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct MemberRequest {
    /// The node's ID, as `sync-node id` prints it.
    pub id: String,
    /// Shown instead of the node ID, on every node.
    #[serde(default)]
    #[cfg_attr(test, ts(optional))]
    pub name: Option<String>,
}

/// `POST /api/folders/{name}/enabled`, for a folder this node keeps.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct FolderEnabledRequest {
    pub enabled: bool,
}

/// `POST /api/members/remove`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct RemoveRequest {
    /// The member's node ID.
    pub id: String,
}

/// `POST /api/group/name`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct GroupNameRequest {
    /// Shown with the group ID, on every node.
    pub name: String,
}

/// `POST /api/folders`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct FolderRequest {
    /// The folder's name in the group, the same on every node.
    pub name: String,
    /// The directory on this node: an absolute path.
    pub path: String,
}

/// `POST /api/members`, `/api/members/remove`, `/api/group/name`,
/// `/api/folders`, `/api/folders/{name}/enabled` and
/// `/api/folders/{name}/remove`: queued for the running node, which applies
/// it within `within_secs`.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct QueuedAnswer {
    pub queued: bool,
    pub within_secs: u64,
}

impl QueuedAnswer {
    fn new() -> (StatusCode, Json<Self>) {
        (
            StatusCode::ACCEPTED,
            Json(Self {
                queued: true,
                within_secs: PRESENCE_INTERVAL.as_secs(),
            }),
        )
    }
}

pub(super) async fn create_group(
    State(state): State<AppState>,
    body: Result<Json<CreateRequest>, JsonRejection>,
) -> Result<Json<CreateAnswer>, ApiError> {
    let request = parse(body)?;
    let name = checked_name(request.name.as_deref())?;
    not_in_group(&*state.backend)?;
    let backend = state.backend.clone();
    let task = state
        .topics
        .spawn(async move { backend.create_group(name).await });
    let group = finish(task).await?;
    Ok(Json(CreateAnswer {
        group: group.to_string(),
    }))
}

pub(super) async fn join(
    State(state): State<AppState>,
    body: Result<Json<JoinRequest>, JsonRejection>,
) -> Result<Json<JoinAnswer>, ApiError> {
    let request = parse(body)?;
    let ticket: Ticket = request
        .ticket
        .parse()
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, format!("{err}")))?;
    not_in_group(&*state.backend)?;
    let backend = state.backend.clone();
    let task = state
        .topics
        .spawn(async move { backend.join(ticket).await });
    match tokio::time::timeout(JOIN_LIMIT, finish(task)).await {
        Ok(joined) => {
            let (group, founder) = joined?;
            Ok(Json(JoinAnswer {
                group: group.to_string(),
                founder: founder.to_string(),
            }))
        }
        Err(_) => Err(ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            format!(
                "the join didn't finish within {}s; it goes on, and the page shows the group once it has",
                JOIN_LIMIT.as_secs()
            ),
        )),
    }
}

pub(super) async fn create_invite(
    State(state): State<AppState>,
    body: Result<Json<InviteRequest>, JsonRejection>,
) -> Result<Json<InviteAnswer>, ApiError> {
    let request = parse(body)?;
    let group = in_group(&*state.backend)?;
    let name = checked_name(request.name.as_deref())?;
    let hours = request.hours.unwrap_or(24);
    // As `invite create`: the ticket carries only the ID, and the joiner
    // finds this node's address through n0's DNS.
    let ticket = invite::create(
        &state.config.config_dir,
        EndpointAddr::new(state.backend.id()),
        group,
        Duration::from_secs(hours.saturating_mul(3600)),
        name.as_deref(),
    )
    .map_err(anyhow::Error::from)?;
    Ok(Json(InviteAnswer {
        ticket: ticket.to_string(),
    }))
}

pub(super) async fn add_member(
    State(state): State<AppState>,
    body: Result<Json<MemberRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<QueuedAnswer>), ApiError> {
    let request = parse(body)?;
    let id = node_id(&request.id)?;
    in_group(&*state.backend)?;
    let name = checked_name(request.name.as_deref())?;
    member::queue_add(&state.config.config_dir, id, name.as_deref())
        .map_err(anyhow::Error::from)?;
    Ok(QueuedAnswer::new())
}

pub(super) async fn remove_member(
    State(state): State<AppState>,
    body: Result<Json<RemoveRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<QueuedAnswer>), ApiError> {
    let request = parse(body)?;
    let id = node_id(&request.id)?;
    let status = state.backend.status().ok_or(GroupError::NotInGroup)?;
    let bad = |text: String| ApiError::new(StatusCode::BAD_REQUEST, text);
    check_removable(id, status.node, status.founder).map_err(|err| bad(err.to_string()))?;
    // Unlike the CLI, which has only the group file, the page knows the
    // members.
    if status.member(id).is_none() {
        return Err(bad(format!("{} is not a member", id.fmt_short())));
    }
    member::queue_remove(&state.config.config_dir, id).map_err(anyhow::Error::from)?;
    Ok(QueuedAnswer::new())
}

pub(super) async fn name_group(
    State(state): State<AppState>,
    body: Result<Json<GroupNameRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<QueuedAnswer>), ApiError> {
    let request = parse(body)?;
    in_group(&*state.backend)?;
    let name = check_name(&request.name)
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
    member::queue_group_name(&state.config.config_dir, &name).map_err(anyhow::Error::from)?;
    Ok(QueuedAnswer::new())
}

pub(super) async fn add_folder(
    State(state): State<AppState>,
    body: Result<Json<FolderRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<QueuedAnswer>), ApiError> {
    let request = parse(body)?;
    in_group(&*state.backend)?;
    let bad = |text: String| ApiError::new(StatusCode::BAD_REQUEST, text);
    let name = folder::check_folder_name(&request.name).map_err(|err| bad(err.to_string()))?;
    // As `Folders::add` would, but now rather than once the queue runs.
    if let Some(kept) = folder::load_local(&state.config.config_dir)?
        .into_iter()
        .find(|f| f.name == name)
    {
        return Err(bad(format!(
            "this node already keeps folder {name} at {}",
            kept.path.display()
        )));
    }
    let path = PathBuf::from(&request.path);
    // The node's working directory means nothing on the page.
    if !path.is_absolute() {
        return Err(bad(format!("{} is not an absolute path", path.display())));
    }
    let path = path
        .canonicalize()
        .map_err(|err| bad(format!("resolving {}: {err}", path.display())))?;
    if !path.is_dir() {
        return Err(bad(format!("{} is not a directory", path.display())));
    }
    folder::queue_add(&state.config.config_dir, &name, &path).map_err(anyhow::Error::from)?;
    Ok(QueuedAnswer::new())
}

pub(super) async fn set_folder_enabled(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Result<Json<FolderEnabledRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<QueuedAnswer>), ApiError> {
    let request = parse(body)?;
    in_group(&*state.backend)?;
    let bad = |text: String| ApiError::new(StatusCode::BAD_REQUEST, text);
    let name = folder::check_folder_name(&name).map_err(|err| bad(err.to_string()))?;
    // A name that isn't kept is the caller's mistake, as with the CLI.
    if !folder::load_local(&state.config.config_dir)?
        .iter()
        .any(|f| f.name == name)
    {
        return Err(bad(format!("this node doesn't keep folder {name}")));
    }
    folder::set_enabled(&state.config.config_dir, &name, request.enabled)?;
    Ok(QueuedAnswer::new())
}

/// `POST /api/folders/{name}/remove`, with an empty JSON body.
pub(super) async fn remove_folder(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<QueuedAnswer>), ApiError> {
    in_group(&*state.backend)?;
    let bad = |text: String| ApiError::new(StatusCode::BAD_REQUEST, text);
    let name = folder::check_folder_name(&name).map_err(|err| bad(err.to_string()))?;
    if !folder::load_local(&state.config.config_dir)?
        .iter()
        .any(|f| f.name == name)
    {
        return Err(bad(format!("this node doesn't keep folder {name}")));
    }
    folder::queue_remove(&state.config.config_dir, &name)?;
    Ok(QueuedAnswer::new())
}

/// The request body, or 400 with why it isn't one.
fn parse<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    body.map(|Json(body)| body)
        .map_err(|rejection| ApiError::new(StatusCode::BAD_REQUEST, rejection.body_text()))
}

/// A node ID, or 400.
fn node_id(text: &str) -> Result<EndpointId, ApiError> {
    text.trim()
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, format!("not a node ID: {text}")))
}

fn checked_name(name: Option<&str>) -> Result<Option<String>, ApiError> {
    name.map(check_name)
        .transpose()
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err.to_string()))
}

/// This node's group, or 409.
fn in_group(backend: &dyn UiBackend) -> Result<GroupId, ApiError> {
    backend
        .status()
        .map(|status| status.group)
        .ok_or_else(|| GroupError::NotInGroup.into())
}

/// 409 if this node is in a group.
fn not_in_group(backend: &dyn UiBackend) -> Result<(), ApiError> {
    match backend.status() {
        Some(status) => Err(GroupError::AlreadyInGroup(status.group).into()),
        None => Ok(()),
    }
}

/// An action task's result, or 503 if the node stopped first.
async fn finish<T>(task: JoinHandle<Option<anyhow::Result<T>>>) -> Result<T, ApiError> {
    match task.await {
        Ok(Some(result)) => result.map_err(ApiError::from),
        Ok(None) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the node is stopping",
        )),
        Err(err) => Err(anyhow::Error::from(err).into()),
    }
}
