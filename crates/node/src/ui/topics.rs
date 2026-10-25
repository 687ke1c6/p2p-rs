//! UI topics (ADR 0005): the page's reads, streamed over its WebSocket.
//!
//! The node has no change events for this data, so each UI topic is read on
//! a timer. A topic's timer runs only while something subscribes to it, and
//! one timer serves every subscription, on every connection. A tick sends
//! only if the answer differs from the last one sent. Subscriptions hold the
//! latest message only, so a slow connection skips to it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TrackedFuture;
use tokio_util::task::TaskTracker;

use super::{
    read_folder, read_folders, read_status, read_transfers, ErrorAnswer, FolderInfo,
    FolderViewAnswer, StatusAnswer, TransfersAnswer, UiBackend,
};

/// How often a subscribed UI topic is read.
pub const TOPIC_INTERVAL: Duration = Duration::from_secs(2);

/// What the page sends over the WebSocket: `{ "subscribe": "status" }`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe(String),
    Unsubscribe(String),
}

/// What the server sends over the WebSocket: a UI topic's new data.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
pub struct ServerMessage {
    pub topic: String,
    pub update: Update,
}

/// A UI topic's data: the answer of its read route, or its error.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[serde(tag = "kind", content = "data")]
pub enum Update {
    Status(StatusAnswer),
    Folders(Vec<FolderInfo>),
    Folder(FolderViewAnswer),
    Transfers(TransfersAnswer),
    Error(ErrorAnswer),
}

/// A UI topic, parsed from its name.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Topic {
    Status,
    Folders,
    Folder(String),
    Transfers,
}

impl Topic {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "status" => Some(Self::Status),
            "folders" => Some(Self::Folders),
            "transfers" => Some(Self::Transfers),
            _ => match name.strip_prefix("folders/") {
                Some(folder) if !folder.is_empty() => Some(Self::Folder(folder.to_string())),
                _ => None,
            },
        }
    }

    fn read(&self, backend: &dyn UiBackend) -> Update {
        match self {
            Self::Status => Update::Status(read_status(backend)),
            Self::Folders => Update::Folders(read_folders(backend)),
            Self::Folder(name) => match read_folder(backend, name) {
                Ok(view) => Update::Folder(view),
                Err(err) => Update::Error(ErrorAnswer { error: err.message }),
            },
            Self::Transfers => Update::Transfers(read_transfers(backend)),
        }
    }
}

/// A serialized [`ServerMessage`], shared by every subscription.
pub type Message = Arc<str>;

/// The UI topics of one router, shared by all its connections.
#[derive(Clone)]
pub struct Topics(Arc<Inner>);

struct Inner {
    backend: Arc<dyn UiBackend>,
    interval: Duration,
    /// The topics with a running timer, by name. Each sender holds the last
    /// message sent, `None` until the first read.
    running: Mutex<HashMap<String, Arc<watch::Sender<Option<Message>>>>>,
    stop: CancellationToken,
    /// The timers and the socket loops, so `stop` can wait for them.
    tasks: TaskTracker,
}

impl Topics {
    pub fn new(backend: Arc<dyn UiBackend>, interval: Duration) -> Self {
        Self(Arc::new(Inner {
            backend,
            interval,
            running: Mutex::new(HashMap::new()),
            stop: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }))
    }

    /// Subscribes to UI topic `name`, starting its timer if it has none. An
    /// unknown topic gets its `Error` message instead.
    pub fn subscribe(&self, name: &str) -> Result<Subscription, Message> {
        let Some(topic) = Topic::parse(name) else {
            return Err(message(
                name,
                Update::Error(ErrorAnswer {
                    error: format!("no such UI topic: {name}"),
                }),
            ));
        };
        if self.0.stop.is_cancelled() {
            return Err(message(
                name,
                Update::Error(ErrorAnswer {
                    error: "the node is stopping".to_string(),
                }),
            ));
        }
        let mut running = self.0.running.lock().unwrap();
        let sender = running.entry(name.to_string()).or_insert_with(|| {
            let sender = Arc::new(watch::Sender::new(None));
            self.0
                .tasks
                .spawn(self.clone().tick(name.to_string(), topic, sender.clone()));
            sender
        });
        let mut rx = sender.subscribe();
        // The latest message, if there is one, comes first.
        rx.mark_changed();
        Ok(Subscription(rx))
    }

    /// Reads `topic` every interval while it has a subscription, and sends
    /// what changed.
    async fn tick(self, name: String, topic: Topic, sender: Arc<watch::Sender<Option<Message>>>) {
        tracing::info!("web UI: reading topic {name} every {:?}", self.0.interval);
        let mut timer = tokio::time::interval(self.0.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.0.stop.cancelled() => break,
                _ = sender.closed() => {
                    // A new subscription may have come in since.
                    let mut running = self.0.running.lock().unwrap();
                    if sender.receiver_count() == 0 {
                        running.remove(&name);
                        break;
                    }
                }
                _ = timer.tick() => {
                    let text = message(&name, topic.read(&*self.0.backend));
                    sender.send_if_modified(|last| {
                        if last.as_deref() == Some(&*text) {
                            return false;
                        }
                        *last = Some(text);
                        true
                    });
                }
            }
        }
        tracing::info!("web UI: stopped reading topic {name}");
    }

    /// Runs `future` (a socket loop) as one of the tasks `stop` waits for.
    pub fn track<F>(&self, future: F) -> TrackedFuture<F>
    where
        F: std::future::Future,
    {
        self.0.tasks.track_future(future)
    }

    /// Runs `future` (an action) to its end even if its request goes, unless
    /// the node stops first: then `None`. `stop` waits for it.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<Option<F::Output>>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let stop = self.0.stop.clone();
        self.0.tasks.spawn(async move {
            tokio::select! {
                _ = stop.cancelled() => None,
                output = future => Some(output),
            }
        })
    }

    /// Resolves once `stop` is called.
    pub async fn stopped(&self) {
        self.0.stop.cancelled().await
    }

    /// Stops every timer and action, and asks every socket loop to end, then
    /// waits for them. Their subscriptions end with them.
    pub async fn stop(&self) {
        self.0.stop.cancel();
        self.0.tasks.close();
        self.0.tasks.wait().await;
        self.0.running.lock().unwrap().clear();
    }

    /// How many topics have a running timer.
    pub fn running(&self) -> usize {
        self.0.running.lock().unwrap().len()
    }
}

/// One subscription to a UI topic. Dropping it unsubscribes.
pub struct Subscription(watch::Receiver<Option<Message>>);

impl Subscription {
    /// The latest message not yet returned, waiting for one if needed.
    /// `None` once the topic's timer has stopped.
    pub async fn next(&mut self) -> Option<Message> {
        loop {
            self.0.changed().await.ok()?;
            if let Some(text) = self.0.borrow_and_update().clone() {
                return Some(text);
            }
        }
    }
}

fn message(topic: &str, update: Update) -> Message {
    let message = ServerMessage {
        topic: topic.to_string(),
        update,
    };
    serde_json::to_string(&message)
        .expect("UI messages serialize")
        .into()
}
