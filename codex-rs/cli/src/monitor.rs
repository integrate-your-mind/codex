use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use std::time::SystemTime;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::response::IntoResponse;
use axum::routing::get;
use clap::Args;
use codex_common::CliConfigOverrides;
use codex_core::RolloutRecorder;
use codex_core::config::Config;
use codex_core::config::ConfigOverrides;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageContentDeltaEvent;
use codex_protocol::protocol::AgentMessageDeltaEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ItemStartedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::TaskCompleteEvent;
use codex_protocol::protocol::TaskStartedEvent;
use codex_protocol::protocol::TurnContextItem;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Args)]
pub struct MonitorCommand {
    /// Host/interface to bind for the WebSocket server.
    #[arg(long = "host", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to bind for the WebSocket server.
    #[arg(long = "port", default_value_t = 8787)]
    pub port: i32,

    /// Poll interval (ms) for scanning active sessions.
    #[arg(long = "poll-interval-ms", default_value_t = 1000)]
    pub poll_interval_ms: i64,

    /// Treat sessions as active if updated within this window (seconds).
    #[arg(long = "active-window-seconds", default_value_t = 120)]
    pub active_window_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SessionSnapshot {
    sessions: Vec<ActiveSession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ActiveSession {
    session_id: String,
    thread_id: Option<String>,
    turn_id: Option<String>,
    cwd: Option<String>,
    last_response: Option<String>,
    updated_at: Option<String>,
    rollout_path: String,
}

#[derive(Clone)]
struct MonitorState {
    latest: Arc<RwLock<Option<SessionSnapshot>>>,
    updates: broadcast::Sender<SessionSnapshot>,
}

pub async fn run_monitor(
    cmd: MonitorCommand,
    root_config_overrides: CliConfigOverrides,
    config_profile: Option<String>,
) -> Result<()> {
    let poll_interval = validate_poll_interval(cmd.poll_interval_ms)?;
    let active_window = validate_active_window(cmd.active_window_seconds)?;
    let addr = build_socket_addr(&cmd.host, cmd.port)?;
    let (codex_home, default_provider) =
        resolve_monitor_config(root_config_overrides, config_profile).await?;

    let (updates, _) = broadcast::channel(16);
    let latest = Arc::new(RwLock::new(None));
    let state = MonitorState {
        latest: latest.clone(),
        updates: updates.clone(),
    };

    tokio::spawn(monitor_loop(
        codex_home,
        default_provider,
        poll_interval,
        active_window,
        latest,
        updates,
    ));

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = TcpListener::bind(addr)
        .await
        .context("failed to bind monitor websocket")?;

    eprintln!(
        "Codex monitor websocket listening on ws://{}",
        listener.local_addr()?
    );

    axum::serve(listener, app).await?;
    Ok(())
}

async fn resolve_monitor_config(
    root_config_overrides: CliConfigOverrides,
    config_profile: Option<String>,
) -> Result<(PathBuf, String)> {
    let cli_kv_overrides = root_config_overrides
        .parse_overrides()
        .map_err(anyhow::Error::msg)?;
    let overrides = ConfigOverrides {
        config_profile,
        ..Default::default()
    };

    match Config::load_with_cli_overrides(cli_kv_overrides, overrides).await {
        Ok(config) => Ok((config.codex_home, config.model_provider_id)),
        Err(err) => {
            eprintln!("monitor: failed to load config ({err}); falling back to CODEX_HOME");
            let codex_home =
                codex_core::config::find_codex_home().context("failed to resolve CODEX_HOME")?;
            Ok((codex_home, "openai".to_string()))
        }
    }
}

async fn ws_handler(State(state): State<MonitorState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: MonitorState) {
    let snapshot = state
        .latest
        .read()
        .await
        .clone()
        .unwrap_or_else(|| SessionSnapshot {
            sessions: Vec::new(),
        });
    if send_snapshot(&mut socket, &snapshot).await.is_err() {
        return;
    }

    let mut rx = state.updates.subscribe();
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(snapshot) => {
                        if send_snapshot(&mut socket, &snapshot).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(_) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {},
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

async fn send_snapshot(socket: &mut WebSocket, snapshot: &SessionSnapshot) -> Result<()> {
    let payload = serde_json::to_string(snapshot)?;
    socket.send(Message::Text(payload.into())).await?;
    Ok(())
}

async fn monitor_loop(
    codex_home: PathBuf,
    default_provider: String,
    poll_interval: Duration,
    active_window: StdDuration,
    latest: Arc<RwLock<Option<SessionSnapshot>>>,
    updates: broadcast::Sender<SessionSnapshot>,
) {
    let mut ticker = tokio::time::interval_at(Instant::now(), poll_interval);
    let mut last_snapshot: Option<SessionSnapshot> = None;

    loop {
        ticker.tick().await;
        match build_snapshot(&codex_home, &default_provider, active_window).await {
            Ok(snapshot) => {
                if last_snapshot.as_ref() != Some(&snapshot) {
                    *latest.write().await = Some(snapshot.clone());
                    let _ = updates.send(snapshot.clone());
                    last_snapshot = Some(snapshot);
                }
            }
            Err(err) => {
                eprintln!("monitor scan failed: {err}");
            }
        }
    }
}

async fn build_snapshot(
    codex_home: &Path,
    default_provider: &str,
    active_window: StdDuration,
) -> Result<SessionSnapshot> {
    let mut sessions = Vec::new();
    let mut cursor = None;

    loop {
        let page = RolloutRecorder::list_conversations(
            codex_home,
            200,
            cursor.as_ref(),
            &[],
            None,
            default_provider,
        )
        .await
        .context("failed to list conversations")?;

        for item in page.items {
            if let Some(status) = read_rollout_status(&item.path, active_window)
                .await
                .with_context(|| format!("failed to read rollout {}", item.path.display()))?
            {
                sessions.push(status);
            }
        }

        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    Ok(SessionSnapshot { sessions })
}

async fn read_rollout_status(
    path: &Path,
    active_window: StdDuration,
) -> Result<Option<ActiveSession>> {
    let modified = tokio::fs::metadata(path)
        .await
        .ok()
        .and_then(|meta| meta.modified().ok());
    let contents = tokio::fs::read_to_string(path).await?;
    Ok(status_from_lines_with_window(
        path,
        contents.lines(),
        modified,
        active_window,
    ))
}

#[cfg(test)]
fn status_from_lines<'a>(
    path: &Path,
    lines: impl Iterator<Item = &'a str>,
) -> Option<ActiveSession> {
    status_from_lines_with_window(path, lines, None, StdDuration::from_secs(0))
}

fn status_from_lines_with_window<'a>(
    path: &Path,
    lines: impl Iterator<Item = &'a str>,
    modified: Option<SystemTime>,
    active_window: StdDuration,
) -> Option<ActiveSession> {
    let mut tracker = RolloutStatusTracker::new(path, modified, active_window);
    for line in lines {
        let Ok(entry) = serde_json::from_str::<RolloutLine>(line) else {
            continue;
        };
        tracker.apply_line(entry);
    }
    tracker.finish()
}

struct RolloutStatusTracker {
    path: PathBuf,
    modified: Option<SystemTime>,
    active_window: StdDuration,
    session_id: Option<String>,
    thread_id: Option<String>,
    turn_id: Option<String>,
    cwd: Option<String>,
    last_response: Option<String>,
    streaming_response: Option<String>,
    updated_at: Option<String>,
    active: bool,
    saw_completion: bool,
}

impl RolloutStatusTracker {
    fn new(path: &Path, modified: Option<SystemTime>, active_window: StdDuration) -> Self {
        Self {
            path: path.to_path_buf(),
            modified,
            active_window,
            session_id: None,
            thread_id: None,
            turn_id: None,
            cwd: None,
            last_response: None,
            streaming_response: None,
            updated_at: None,
            active: false,
            saw_completion: false,
        }
    }

    fn apply_line(&mut self, line: RolloutLine) {
        self.updated_at = Some(line.timestamp.clone());
        match line.item {
            RolloutItem::SessionMeta(meta) => self.apply_session_meta(meta),
            RolloutItem::TurnContext(context) => self.apply_turn_context(context),
            RolloutItem::Compacted(compacted) => {
                self.last_response = Some(compacted.message);
            }
            RolloutItem::ResponseItem(item) => self.apply_response_item(item),
            RolloutItem::EventMsg(event) => self.apply_event(event),
        }
    }

    fn apply_session_meta(&mut self, meta: SessionMetaLine) {
        self.session_id = Some(meta.meta.id.to_string());
        self.cwd = Some(meta.meta.cwd.to_string_lossy().to_string());
    }

    fn apply_turn_context(&mut self, context: TurnContextItem) {
        self.cwd = Some(context.cwd.to_string_lossy().to_string());
    }

    fn apply_response_item(&mut self, item: ResponseItem) {
        if let ResponseItem::Message { role, content, .. } = item {
            if role != "assistant" {
                return;
            }
            let text = content
                .into_iter()
                .filter_map(|item| match item {
                    ContentItem::OutputText { text } => Some(text),
                    _ => None,
                })
                .collect::<Vec<String>>()
                .join("");
            if !text.is_empty() {
                self.last_response = Some(text.clone());
                if self.active {
                    self.streaming_response = Some(text);
                }
            }
        }
    }

    fn apply_event(&mut self, event: EventMsg) {
        match event {
            EventMsg::TaskStarted(event) => self.apply_task_started(event),
            EventMsg::TaskComplete(event) => self.apply_task_complete(event),
            EventMsg::TurnAborted(_) => self.apply_turn_aborted(),
            EventMsg::AgentMessage(event) => {
                self.last_response = Some(event.message.clone());
                if self.active {
                    self.streaming_response = Some(event.message);
                }
            }
            EventMsg::AgentMessageDelta(event) => self.apply_message_delta(event),
            EventMsg::AgentMessageContentDelta(event) => self.apply_message_content_delta(event),
            EventMsg::ItemStarted(event) => self.apply_item_started(event),
            EventMsg::ItemCompleted(event) => self.apply_item_completed(event),
            _ => {}
        }
    }

    fn apply_task_started(&mut self, _event: TaskStartedEvent) {
        self.active = true;
        self.last_response = None;
        self.streaming_response = Some(String::new());
        self.turn_id = None;
        self.thread_id = None;
    }

    fn apply_task_complete(&mut self, event: TaskCompleteEvent) {
        self.active = false;
        self.saw_completion = true;
        if let Some(message) = event.last_agent_message {
            self.last_response = Some(message);
        } else if let Some(streaming) = self.streaming_response.take()
            && !streaming.is_empty()
        {
            self.last_response = Some(streaming);
        }
        self.streaming_response = None;
    }

    fn apply_turn_aborted(&mut self) {
        self.active = false;
        self.saw_completion = true;
        if let Some(streaming) = self.streaming_response.take()
            && !streaming.is_empty()
        {
            self.last_response = Some(streaming);
        }
        self.streaming_response = None;
    }

    fn apply_message_delta(&mut self, event: AgentMessageDeltaEvent) {
        if !self.active {
            return;
        }
        let buffer = self.streaming_response.get_or_insert_with(String::new);
        buffer.push_str(&event.delta);
    }

    fn apply_message_content_delta(&mut self, event: AgentMessageContentDeltaEvent) {
        if !self.active {
            return;
        }
        self.thread_id = Some(event.thread_id);
        self.turn_id = Some(event.turn_id);
        let buffer = self.streaming_response.get_or_insert_with(String::new);
        buffer.push_str(&event.delta);
    }

    fn apply_item_started(&mut self, event: ItemStartedEvent) {
        self.thread_id = Some(event.thread_id.to_string());
        self.turn_id = Some(event.turn_id);
    }

    fn apply_item_completed(&mut self, event: ItemCompletedEvent) {
        self.thread_id = Some(event.thread_id.to_string());
        self.turn_id = Some(event.turn_id);
        if let codex_protocol::items::TurnItem::AgentMessage(item) = event.item {
            let text = item
                .content
                .into_iter()
                .map(|content| match content {
                    codex_protocol::items::AgentMessageContent::Text { text } => text,
                })
                .collect::<Vec<String>>()
                .join("");
            if !text.is_empty() {
                self.last_response = Some(text.clone());
                if self.active {
                    self.streaming_response = Some(text);
                }
            }
        }
    }

    fn finish(mut self) -> Option<ActiveSession> {
        if !self.active {
            let recently_active = self
                .modified
                .and_then(|modified| modified.elapsed().ok())
                .map_or(false, |elapsed| elapsed <= self.active_window);
            if self.saw_completion || !recently_active {
                return None;
            }
            self.active = true;
        }

        if let Some(streaming) = &self.streaming_response
            && !streaming.is_empty()
        {
            self.last_response = Some(streaming.clone());
        }

        let session_id = self
            .session_id
            .or_else(|| self.thread_id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        Some(ActiveSession {
            session_id,
            thread_id: self.thread_id,
            turn_id: self.turn_id,
            cwd: self.cwd,
            last_response: self.last_response,
            updated_at: self.updated_at,
            rollout_path: self.path.to_string_lossy().to_string(),
        })
    }
}

fn build_socket_addr(host: &str, port: i32) -> Result<SocketAddr> {
    if port <= 0 || port > 65535 {
        return Err(anyhow!("port must be between 1 and 65535"));
    }
    let port = u16::try_from(port).context("invalid port")?;
    let addr = format!("{host}:{port}")
        .parse::<SocketAddr>()
        .context("invalid host or port")?;
    Ok(addr)
}

fn validate_poll_interval(poll_interval_ms: i64) -> Result<Duration> {
    if poll_interval_ms <= 0 {
        return Err(anyhow!("poll interval must be > 0"));
    }
    let millis = u64::try_from(poll_interval_ms).context("poll interval too large")?;
    Ok(Duration::from_millis(millis))
}

fn validate_active_window(active_window_seconds: i64) -> Result<StdDuration> {
    if active_window_seconds <= 0 {
        return Err(anyhow!("active window must be > 0"));
    }
    let secs = u64::try_from(active_window_seconds).context("active window too large")?;
    Ok(StdDuration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::ConversationId;
    use codex_protocol::items::AgentMessageContent;
    use codex_protocol::items::AgentMessageItem;
    use codex_protocol::items::TurnItem;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::TurnAbortReason;
    use codex_protocol::protocol::TurnAbortedEvent;
    use pretty_assertions::assert_eq;

    fn rollout_line(timestamp: &str, item: RolloutItem) -> String {
        serde_json::to_string(&RolloutLine {
            timestamp: timestamp.to_string(),
            item,
        })
        .expect("serialize rollout line")
    }

    #[test]
    fn status_from_lines_reports_active_with_latest_response() {
        let conversation_id = ConversationId::new();
        let meta = RolloutItem::SessionMeta(SessionMetaLine {
            meta: codex_protocol::protocol::SessionMeta {
                id: conversation_id,
                timestamp: "2025-01-01T00:00:00Z".to_string(),
                cwd: PathBuf::from("/tmp"),
                originator: "codex_cli_rs".to_string(),
                cli_version: "0.0.0".to_string(),
                instructions: None,
                source: codex_protocol::protocol::SessionSource::default(),
                model_provider: None,
            },
            git: None,
        });

        let lines = [
            rollout_line("2025-01-01T00:00:00Z", meta),
            rollout_line(
                "2025-01-01T00:00:01Z",
                RolloutItem::EventMsg(EventMsg::TaskStarted(TaskStartedEvent {
                    model_context_window: None,
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:02Z",
                RolloutItem::EventMsg(EventMsg::ItemStarted(ItemStartedEvent {
                    thread_id: conversation_id,
                    turn_id: "turn-1".to_string(),
                    item: TurnItem::AgentMessage(AgentMessageItem {
                        id: "item-1".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "Hello".to_string(),
                        }],
                    }),
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:03Z",
                RolloutItem::EventMsg(EventMsg::AgentMessageContentDelta(
                    AgentMessageContentDeltaEvent {
                        thread_id: conversation_id.to_string(),
                        turn_id: "turn-1".to_string(),
                        item_id: "item-1".to_string(),
                        delta: "Hello".to_string(),
                    },
                )),
            ),
            rollout_line(
                "2025-01-01T00:00:04Z",
                RolloutItem::EventMsg(EventMsg::AgentMessageContentDelta(
                    AgentMessageContentDeltaEvent {
                        thread_id: conversation_id.to_string(),
                        turn_id: "turn-1".to_string(),
                        item_id: "item-1".to_string(),
                        delta: " world".to_string(),
                    },
                )),
            ),
        ];

        let status = status_from_lines(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
        )
        .expect("expected active session");

        assert_eq!(
            status,
            ActiveSession {
                session_id: conversation_id.to_string(),
                thread_id: Some(conversation_id.to_string()),
                turn_id: Some("turn-1".to_string()),
                cwd: Some("/tmp".to_string()),
                last_response: Some("Hello world".to_string()),
                updated_at: Some("2025-01-01T00:00:04Z".to_string()),
                rollout_path: "/rollout.jsonl".to_string(),
            }
        );
    }

    #[test]
    fn status_from_lines_ignores_completed_turns() {
        let conversation_id = ConversationId::new();
        let lines = [
            rollout_line(
                "2025-01-01T00:00:00Z",
                RolloutItem::EventMsg(EventMsg::TaskStarted(TaskStartedEvent {
                    model_context_window: None,
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:01Z",
                RolloutItem::EventMsg(EventMsg::TaskComplete(TaskCompleteEvent {
                    last_agent_message: Some("done".to_string()),
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:02Z",
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    thread_id: conversation_id,
                    turn_id: "turn-2".to_string(),
                    item: TurnItem::AgentMessage(AgentMessageItem {
                        id: "item-2".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "done".to_string(),
                        }],
                    }),
                })),
            ),
        ];

        let status = status_from_lines(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
        );
        assert_eq!(status, None);
    }

    #[test]
    fn status_from_lines_marks_recent_activity_as_active() {
        let conversation_id = ConversationId::new();
        let meta = RolloutItem::SessionMeta(SessionMetaLine {
            meta: codex_protocol::protocol::SessionMeta {
                id: conversation_id,
                timestamp: "2025-01-01T00:00:00Z".to_string(),
                cwd: PathBuf::from("/tmp"),
                originator: "codex_cli_rs".to_string(),
                cli_version: "0.0.0".to_string(),
                instructions: None,
                source: SessionSource::default(),
                model_provider: None,
            },
            git: None,
        });

        let lines = [rollout_line("2025-01-01T00:00:00Z", meta)];
        let status = status_from_lines_with_window(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
            Some(SystemTime::now()),
            StdDuration::from_secs(120),
        )
        .expect("expected active session");

        assert_eq!(status.session_id, conversation_id.to_string());
        assert_eq!(status.cwd, Some("/tmp".to_string()));
    }

    #[test]
    fn status_from_lines_recent_activity_prefers_active_flag() {
        let conversation_id = ConversationId::new();
        let lines = [
            rollout_line(
                "2025-01-01T00:00:00Z",
                RolloutItem::SessionMeta(SessionMetaLine {
                    meta: codex_protocol::protocol::SessionMeta {
                        id: conversation_id,
                        timestamp: "2025-01-01T00:00:00Z".to_string(),
                        cwd: PathBuf::from("/tmp"),
                        originator: "codex_cli_rs".to_string(),
                        cli_version: "0.0.0".to_string(),
                        instructions: None,
                        source: SessionSource::default(),
                        model_provider: None,
                    },
                    git: None,
                }),
            ),
            rollout_line(
                "2025-01-01T00:00:01Z",
                RolloutItem::EventMsg(EventMsg::TaskStarted(TaskStartedEvent {
                    model_context_window: None,
                })),
            ),
        ];

        let old_time = SystemTime::now()
            .checked_sub(StdDuration::from_secs(10_000))
            .expect("old time");
        let status = status_from_lines_with_window(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
            Some(old_time),
            StdDuration::from_secs(120),
        )
        .expect("expected active session");

        assert_eq!(status.session_id, conversation_id.to_string());
    }

    #[test]
    fn status_from_lines_recent_activity_skips_completed_sessions() {
        let conversation_id = ConversationId::new();
        let lines = [
            rollout_line(
                "2025-01-01T00:00:00Z",
                RolloutItem::EventMsg(EventMsg::TaskStarted(TaskStartedEvent {
                    model_context_window: None,
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:01Z",
                RolloutItem::EventMsg(EventMsg::TaskComplete(TaskCompleteEvent {
                    last_agent_message: Some("done".to_string()),
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:02Z",
                RolloutItem::SessionMeta(SessionMetaLine {
                    meta: codex_protocol::protocol::SessionMeta {
                        id: conversation_id,
                        timestamp: "2025-01-01T00:00:00Z".to_string(),
                        cwd: PathBuf::from("/tmp"),
                        originator: "codex_cli_rs".to_string(),
                        cli_version: "0.0.0".to_string(),
                        instructions: None,
                        source: SessionSource::default(),
                        model_provider: None,
                    },
                    git: None,
                }),
            ),
        ];

        let status = status_from_lines_with_window(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
            Some(SystemTime::now()),
            StdDuration::from_secs(120),
        );

        assert_eq!(status, None);
    }

    #[test]
    fn status_from_lines_recent_activity_skips_aborted_sessions() {
        let conversation_id = ConversationId::new();
        let lines = [
            rollout_line(
                "2025-01-01T00:00:00Z",
                RolloutItem::EventMsg(EventMsg::TaskStarted(TaskStartedEvent {
                    model_context_window: None,
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:01Z",
                RolloutItem::EventMsg(EventMsg::TurnAborted(TurnAbortedEvent {
                    reason: TurnAbortReason::Interrupted,
                })),
            ),
            rollout_line(
                "2025-01-01T00:00:02Z",
                RolloutItem::SessionMeta(SessionMetaLine {
                    meta: codex_protocol::protocol::SessionMeta {
                        id: conversation_id,
                        timestamp: "2025-01-01T00:00:00Z".to_string(),
                        cwd: PathBuf::from("/tmp"),
                        originator: "codex_cli_rs".to_string(),
                        cli_version: "0.0.0".to_string(),
                        instructions: None,
                        source: SessionSource::default(),
                        model_provider: None,
                    },
                    git: None,
                }),
            ),
        ];

        let status = status_from_lines_with_window(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
            Some(SystemTime::now()),
            StdDuration::from_secs(120),
        );

        assert_eq!(status, None);
    }

    #[test]
    fn status_from_lines_recent_activity_ignores_old_sessions() {
        let conversation_id = ConversationId::new();
        let lines = [rollout_line(
            "2025-01-01T00:00:00Z",
            RolloutItem::SessionMeta(SessionMetaLine {
                meta: codex_protocol::protocol::SessionMeta {
                    id: conversation_id,
                    timestamp: "2025-01-01T00:00:00Z".to_string(),
                    cwd: PathBuf::from("/tmp"),
                    originator: "codex_cli_rs".to_string(),
                    cli_version: "0.0.0".to_string(),
                    instructions: None,
                    source: SessionSource::default(),
                    model_provider: None,
                },
                git: None,
            }),
        )];

        let old_time = SystemTime::now()
            .checked_sub(StdDuration::from_secs(10_000))
            .expect("old time");
        let status = status_from_lines_with_window(
            Path::new("/rollout.jsonl"),
            lines.iter().map(std::string::String::as_str),
            Some(old_time),
            StdDuration::from_secs(120),
        );

        assert_eq!(status, None);
    }
}
