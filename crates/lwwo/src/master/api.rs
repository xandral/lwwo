//! The HTTP/JSON API, for the CLI, the TUI and scripts.
//!
//! Every handler turns its request into a [`Call`] for the master's loop
//! and returns the answer: the API itself knows nothing and never touches
//! SQLite. Two answers are streams: `/events`, one line whenever something
//! changes, and the logs of an attempt that is still running.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};

use super::logs::{LogKey, LogStore};
use super::state::{ApiError, Call, Msg, RerunRequest, call};
use crate::security::TokenKind;

/// What every handler needs.
#[derive(Debug, Clone)]
pub(crate) struct Api {
    pub(crate) tx: mpsc::Sender<Msg>,
    pub(crate) logs: Arc<LogStore>,
    pub(crate) changes: broadcast::Sender<()>,
    /// Whether requests need a token.
    pub(crate) auth: bool,
    /// The database file, for backups.
    pub(crate) db: std::path::PathBuf,
    /// The only directory backups are written to.
    pub(crate) backup_dir: std::path::PathBuf,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(json!({ "error": self.message }))).into_response()
    }
}

type Answer = Result<Json<Value>, ApiError>;

async fn ask(api: &Api, request: Call) -> Answer {
    call(&api.tx, request).await.map(Json)
}

/// Why a request to an API without tokens is refused, if it is.
///
/// Without tokens the API listens on a local address only, but a browser
/// on the same machine can reach it too: any web page can send it a form
/// or a `fetch`, and a DAG is a command to run. Browsers say where such a
/// request comes from in `Origin`, and a page that points its own name at
/// 127.0.0.1 (DNS rebinding) still sends that name in `Host`. The CLI and
/// the TUI send neither.
fn not_local(headers: &axum::http::HeaderMap) -> Option<&'static str> {
    if headers.contains_key(header::ORIGIN) {
        return Some(
            "this API takes no requests from web pages: \
             it runs without tokens, for the lwwo command line on this machine",
        );
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    // Without the port; an IPv6 address keeps its brackets.
    let name = match host.rfind(':') {
        Some(colon) if !host.ends_with(']') => &host[..colon],
        _ => host,
    };
    if matches!(name, "127.0.0.1" | "localhost" | "[::1]") {
        None
    } else {
        Some(
            "this API runs without tokens and answers only as 127.0.0.1 or localhost: \
             start the master with tokens to reach it by another name",
        )
    }
}

/// Lets a request through only with a valid API token, or, without
/// tokens, only if it comes from this machine and not from a browser.
async fn authenticate(State(api): State<Api>, request: Request, next: Next) -> Response {
    if !api.auth
        && let Some(message) = not_local(request.headers())
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": message }))).into_response();
    }
    if api.auth {
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_owned);
        let valid = match token {
            Some(token) => call(&api.tx, Call::CheckToken(token)).await,
            None => Ok(json!(false)),
        };
        if valid.ok() != Some(json!(true)) {
            let message = "a valid API token is required: set LWWO_TOKEN";
            return (StatusCode::UNAUTHORIZED, Json(json!({ "error": message }))).into_response();
        }
    }
    next.run(request).await
}

#[derive(Debug, Deserialize)]
struct RunsQuery {
    dag: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerBody {
    at: Option<String>,
    #[serde(default)]
    params: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RerunBody {
    task: Option<String>,
    all: bool,
    only: bool,
    current: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenBody {
    kind: String,
    label: Option<String>,
    #[serde(default)]
    rotate: bool,
    /// For an API token: how long it lasts, such as `90d`.
    expires: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    attempt: Option<u32>,
    #[serde(default)]
    follow: bool,
}

async fn apply(State(api): State<Api>, yaml: String) -> Answer {
    ask(&api, Call::Apply { yaml }).await
}

async fn dags(State(api): State<Api>) -> Answer {
    ask(&api, Call::Dags).await
}

async fn dag(State(api): State<Api>, Path(name): Path<String>) -> Answer {
    ask(&api, Call::Dag(name)).await
}

async fn delete_dag(State(api): State<Api>, Path(name): Path<String>) -> Answer {
    ask(&api, Call::DeleteDag(name)).await
}

async fn pause(State(api): State<Api>, Path(dag): Path<String>) -> Answer {
    ask(&api, Call::Pause { dag, paused: true }).await
}

async fn resume(State(api): State<Api>, Path(dag): Path<String>) -> Answer {
    ask(&api, Call::Pause { dag, paused: false }).await
}

async fn trigger(
    State(api): State<Api>,
    Path(dag): Path<String>,
    body: Option<Json<TriggerBody>>,
) -> Answer {
    let body = body.map(|Json(body)| body).unwrap_or_default();
    // `--param soglia=250` and `{"soglia": 250}` mean the same.
    let params = body
        .params
        .into_iter()
        .map(|(name, value)| match value {
            Value::String(text) => (name, text),
            other => (name, other.to_string()),
        })
        .collect();
    ask(
        &api,
        Call::Trigger {
            dag,
            at: body.at,
            params,
        },
    )
    .await
}

async fn runs(State(api): State<Api>, Query(query): Query<RunsQuery>) -> Answer {
    ask(
        &api,
        Call::Runs {
            dag: query.dag,
            limit: query.limit.unwrap_or(50).min(1_000),
        },
    )
    .await
}

async fn run(State(api): State<Api>, Path(id): Path<String>) -> Answer {
    ask(&api, Call::Run(id)).await
}

async fn cancel(State(api): State<Api>, Path(id): Path<String>) -> Answer {
    ask(&api, Call::Cancel(id)).await
}

async fn rerun(
    State(api): State<Api>,
    Path(run): Path<String>,
    body: Option<Json<RerunBody>>,
) -> Answer {
    let body = body.map(|Json(body)| body).unwrap_or_default();
    let request = RerunRequest {
        task: body.task,
        all: body.all,
        only: body.only,
        current: body.current,
    };
    ask(&api, Call::Rerun { run, request }).await
}

async fn workers(State(api): State<Api>) -> Answer {
    ask(&api, Call::Workers).await
}

async fn drain(State(api): State<Api>, Path(name): Path<String>) -> Answer {
    ask(&api, Call::Drain(name)).await
}

async fn remove_worker(State(api): State<Api>, Path(name): Path<String>) -> Answer {
    ask(&api, Call::RemoveWorker(name)).await
}

async fn new_token(State(api): State<Api>, Json(body): Json<TokenBody>) -> Answer {
    let kind = match body.kind.as_str() {
        "api" => TokenKind::Api,
        "join" | "worker" => TokenKind::Join,
        other => {
            return Err(ApiError {
                status: 400,
                message: format!("unknown kind of token `{other}`: use `api` or `worker`"),
            });
        }
    };
    let expires = match body.expires.as_deref() {
        Some(text) => Some(crate::util::parse_duration(text).map_err(|error| ApiError {
            status: 400,
            message: format!("{error:#}"),
        })?),
        None => None,
    };
    ask(
        &api,
        Call::NewToken {
            kind,
            label: body.label,
            rotate: body.rotate,
            expires,
        },
    )
    .await
}

async fn tokens(State(api): State<Api>) -> Answer {
    ask(&api, Call::Tokens).await
}

async fn revoke_token(State(api): State<Api>, Path(id): Path<String>) -> Answer {
    ask(&api, Call::RevokeToken(id)).await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupBody {
    /// The name of the copy; a name with the time in it if not given.
    name: Option<String>,
}

/// Copies the database into the backup directory.
///
/// The caller chooses a name, never a path: a token must not be enough to
/// make the master write files wherever its user can. And the copy is made
/// on a thread of its own with its own connection, so the master's loop
/// goes on assigning tasks while a large database is copied.
async fn backup(State(api): State<Api>, body: Option<Json<BackupBody>>) -> Answer {
    let fail = |status: u16, message: String| ApiError { status, message };
    let name = body
        .and_then(|Json(body)| body.name)
        .unwrap_or_else(|| format!("lwwo-{}.db", crate::util::now_ms()));
    let plain = !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if !plain {
        return Err(fail(
            400,
            format!(
                "`{name}` is not a name for a backup: letters, digits, `_`, `-` and `.` only. \
                 Backups go to the master's backup directory"
            ),
        ));
    }
    let target = api.backup_dir.join(&name);
    if target.exists() {
        return Err(fail(409, format!("{} exists already", target.display())));
    }
    let (db, path) = (api.db.clone(), target.clone());
    let copied = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
        crate::store::backup(&db, &path)
    })
    .await;
    match copied {
        Ok(Ok(())) => {
            let bytes = std::fs::metadata(&target).map_or(0, |meta| meta.len());
            tracing::info!("database copied to {}", target.display());
            Ok(Json(json!({ "path": target, "bytes": bytes })))
        }
        Ok(Err(error)) => Err(fail(500, format!("the backup failed: {error:#}"))),
        Err(error) => Err(fail(500, format!("the backup failed: {error}"))),
    }
}

async fn status(State(api): State<Api>) -> Answer {
    ask(&api, Call::Status).await
}

/// One line every time something changes, for clients that show live
/// state: they fetch again what they are showing.
async fn events(State(api): State<Api>) -> Response {
    let changes = api.changes.subscribe();
    let lines = stream::unfold(changes, |mut changes| async move {
        match changes.recv().await {
            // Lagging only means several changes became one line.
            Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => Some((
                Ok::<_, Infallible>(Bytes::from_static(b"change\n")),
                changes,
            )),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    });
    Body::from_stream(lines).into_response()
}

/// What a log stream remembers between two pieces.
struct Follow {
    api: Api,
    key: LogKey,
    position: u64,
    /// Keep waiting for more while the attempt runs.
    live: bool,
    /// The attempt is over according to the database.
    finished: bool,
    done: bool,
}

impl Follow {
    /// The next piece of the log, waiting for it if the attempt runs.
    async fn next(mut self) -> Option<(Result<Bytes, std::io::Error>, Self)> {
        loop {
            if self.done {
                return None;
            }
            let (data, closed) = match self.api.logs.read(&self.key, self.position) {
                Ok(read) => read,
                Err(error) => {
                    self.done = true;
                    return Some((Err(error), self));
                }
            };
            self.position += data.len() as u64;
            // Logs come in pieces: as long as there is something, go on.
            if !data.is_empty() {
                return Some((Ok(Bytes::from(data)), self));
            }
            // Nothing more now. Closed: that was everything. Finished
            // without a closed log: the attempt was lost, and no more will
            // come. Not following: what there is was sent.
            if closed || !self.live || self.finished {
                return None;
            }
            // Wait for more output; now and then ask whether the attempt
            // ended without closing its log.
            tokio::select! {
                () = self.api.logs.changed() => {}
                () = tokio::time::sleep(Duration::from_secs(5)) => {
                    let target = Call::LogTarget {
                        run: self.key.run.clone(),
                        task: self.key.task.clone(),
                        attempt: Some(self.key.attempt),
                    };
                    match call(&self.api.tx, target).await {
                        Ok(target) => self.finished = target["finished"] == json!(true),
                        Err(_) => self.done = true,
                    }
                }
            }
        }
    }
}

async fn logs(
    State(api): State<Api>,
    Path((run, task)): Path<(String, String)>,
    Query(query): Query<LogsQuery>,
) -> Result<Response, ApiError> {
    let target = call(
        &api.tx,
        Call::LogTarget {
            run,
            task,
            attempt: query.attempt,
        },
    )
    .await?;
    let text = |field: &str| target[field].as_str().unwrap_or_default().to_owned();
    let attempt = u32::try_from(target["attempt"].as_u64().unwrap_or(0)).unwrap_or(0);
    let follow = Follow {
        api,
        key: LogKey {
            dag: text("dag"),
            run: text("run"),
            task: text("task"),
            attempt,
        },
        position: 0,
        live: query.follow,
        finished: target["finished"] == json!(true),
        done: false,
    };
    let mut response = Body::from_stream(stream::unfold(follow, Follow::next)).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        "text/plain; charset=utf-8".parse().expect("valid"),
    );
    headers.insert("x-lwwo-attempt", attempt.into());
    Ok(response)
}

/// The routes of the API.
pub(crate) fn router(api: Api) -> Router {
    Router::new()
        .route("/dags", get(dags).post(apply))
        .route("/dags/{name}", get(dag).delete(delete_dag))
        .route("/dags/{name}/pause", post(pause))
        .route("/dags/{name}/resume", post(resume))
        .route("/dags/{name}/runs", post(trigger))
        .route("/runs", get(runs))
        .route("/runs/{id}", get(run))
        .route("/runs/{id}/cancel", post(cancel))
        .route("/runs/{id}/rerun", post(rerun))
        .route("/runs/{id}/tasks/{task}/logs", get(logs))
        .route("/workers", get(workers))
        .route("/workers/{name}", axum::routing::delete(remove_worker))
        .route("/workers/{name}/drain", post(drain))
        .route("/tokens", get(tokens).post(new_token))
        .route("/tokens/{id}", axum::routing::delete(revoke_token))
        .route("/status", get(status))
        .route("/backup", post(backup))
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(api.clone(), authenticate))
        .with_state(api)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for &(name, value) in pairs {
            map.insert(name, value.parse().unwrap());
        }
        map
    }

    #[test]
    fn without_tokens_only_local_requests_that_are_not_from_a_browser_pass() {
        for host in [
            "127.0.0.1:7070",
            "localhost:7070",
            "localhost",
            "[::1]:7070",
        ] {
            assert_eq!(not_local(&headers(&[("host", host)])), None, "{host}");
        }
        // A page of another site, and one of this machine: both browsers.
        for origin in ["https://evil.example", "http://localhost:7070", "null"] {
            let request = headers(&[("host", "127.0.0.1:7070"), ("origin", origin)]);
            assert!(not_local(&request).is_some(), "{origin}");
        }
        // A name pointed at 127.0.0.1 by its owner.
        for host in ["evil.example:7070", "127.0.0.1.evil.example", ""] {
            assert!(not_local(&headers(&[("host", host)])).is_some(), "{host}");
        }
        assert!(not_local(&HeaderMap::new()).is_some(), "no Host at all");
    }
}
