//! The commands that talk to a running master and print what it says.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result, bail};
use futures_util::StreamExt as _;
use serde_json::{Value, json};

use crate::client::{Client, LogLine};
use crate::util::{format_local, format_span, now_ms};

/// Prints rows as a table with aligned columns.
fn table(header: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let print = |cells: Vec<&str>| {
        let line: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        println!("{}", line.join("  ").trim_end());
    };
    print(header.to_vec());
    for row in rows {
        print(row.iter().map(String::as_str).collect());
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "-".to_owned(),
        other => other.to_string(),
    }
}

fn time(value: &Value) -> String {
    value.as_i64().map_or_else(|| "-".to_owned(), format_local)
}

/// How long a run or an attempt took, or has been going.
pub(crate) fn duration(started: &Value, finished: &Value) -> String {
    match (started.as_i64(), finished.as_i64()) {
        (Some(start), Some(end)) => format_span(end - start),
        (Some(start), None) => format_span(now_ms() - start),
        _ => "-".to_owned(),
    }
}

/// `lwwo apply <file>`.
pub(crate) async fn apply(client: &Client, file: &Path) -> Result<()> {
    let yaml =
        std::fs::read_to_string(file).with_context(|| format!("cannot read {}", file.display()))?;
    let applied = client.post_text("/dags", &yaml).await?;
    let what = if applied["new_version"] == json!(true) {
        "new version"
    } else {
        "unchanged"
    };
    println!(
        "{}: {what} {}",
        text(&applied["name"]),
        &text(&applied["hash"])[..12]
    );
    match applied["next_fire"].as_i64() {
        Some(next) => println!("next run: {}", format_local(next)),
        None => println!(
            "no schedule: start it with `lwwo run {}`",
            text(&applied["name"])
        ),
    }
    Ok(())
}

/// `lwwo run <dag> [--at …] [--param name=value]…`.
pub(crate) async fn run(
    client: &Client,
    dag: &str,
    at: Option<&str>,
    params: &[String],
) -> Result<()> {
    let mut values = BTreeMap::new();
    for param in params {
        let Some((name, value)) = param.split_once('=') else {
            bail!("`--param {param}` must be written as name=value");
        };
        values.insert(name.to_owned(), value.to_owned());
    }
    let started = client
        .post(
            &format!("/dags/{dag}/runs"),
            &json!({ "at": at, "params": values }),
        )
        .await?;
    println!("{} {}", text(&started["id"]), text(&started["state"]));
    Ok(())
}

/// `lwwo dags`.
pub(crate) async fn dags(client: &Client) -> Result<()> {
    let dags = client.get("/dags").await?;
    let rows: Vec<Vec<String>> = dags
        .as_array()
        .into_iter()
        .flatten()
        .map(|dag| {
            let state = if dag["paused"] == json!(true) {
                "paused"
            } else {
                "active"
            };
            vec![
                text(&dag["name"]),
                state.to_owned(),
                time(&dag["next_fire"]),
                text(&dag["current"])[..12].to_owned(),
            ]
        })
        .collect();
    table(&["DAG", "STATE", "NEXT RUN", "VERSION"], &rows);
    Ok(())
}

/// `lwwo runs [--dag …] [--limit …]`.
pub(crate) async fn runs(client: &Client, dag: Option<&str>, limit: u32) -> Result<()> {
    let filter = dag.map(|dag| format!("&dag={dag}")).unwrap_or_default();
    let runs = client.get(&format!("/runs?limit={limit}{filter}")).await?;
    let rows: Vec<Vec<String>> = runs
        .as_array()
        .into_iter()
        .flatten()
        .map(|run| {
            vec![
                text(&run["id"]),
                text(&run["dag_name"]),
                text(&run["state"]),
                text(&run["trigger"]),
                time(&run["logical_time"]),
                duration(&run["started_at"], &run["finished_at"]),
            ]
        })
        .collect();
    table(
        &["RUN", "DAG", "STATE", "TRIGGER", "RUN TIME", "DURATION"],
        &rows,
    );
    Ok(())
}

/// `lwwo show <run>`.
pub(crate) async fn show(client: &Client, run: &str) -> Result<()> {
    let detail = client.get(&format!("/runs/{run}")).await?;
    let run = &detail["run"];
    println!(
        "{}  {}  {}  {}  run time {}",
        text(&run["id"]),
        text(&run["dag_name"]),
        text(&run["state"]),
        text(&run["trigger"]),
        time(&run["logical_time"]),
    );
    if let Some(problem) = run["problem"].as_str() {
        println!("stopped by lwwo: {problem}");
    }
    if run["params"] != json!("{}") {
        println!("params {}", text(&run["params"]));
    }
    println!();
    let attempts = detail["attempts"].as_array().cloned().unwrap_or_default();
    let rows: Vec<Vec<String>> = detail["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|task| {
            let last = attempts.iter().rev().find(|a| a["task"] == task["task"]);
            let needs: Vec<String> = task["needs"]
                .as_array()
                .into_iter()
                .flatten()
                .map(text)
                .collect();
            vec![
                text(&task["task"]),
                text(&task["state"]),
                text(&task["attempts"]),
                last.map_or_else(|| "-".to_owned(), |a| text(&a["worker"])),
                last.map_or_else(
                    || "-".to_owned(),
                    |a| duration(&a["started_at"], &a["finished_at"]),
                ),
                last.map_or_else(|| "-".to_owned(), |a| text(&a["exit_code"])),
                needs.join(", "),
            ]
        })
        .collect();
    table(
        &[
            "TASK", "STATE", "ATTEMPTS", "WORKER", "DURATION", "EXIT", "NEEDS",
        ],
        &rows,
    );
    // Ready, but no worker takes them: why.
    let waiting: Vec<String> = detail["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|task| {
            let why = task["waiting"].as_str()?;
            Some(format!("{} waits for a worker: {why}", text(&task["task"])))
        })
        .collect();
    if !waiting.is_empty() {
        println!();
        for line in waiting {
            println!("{line}");
        }
    }
    Ok(())
}

/// `lwwo logs <run> <task> [--attempt n] [--follow]`.
pub(crate) async fn logs(
    client: &Client,
    run: &str,
    task: &str,
    attempt: Option<u32>,
    follow: bool,
) -> Result<()> {
    let attempt = attempt.map(|n| format!("&attempt={n}")).unwrap_or_default();
    let response = client
        .stream(&format!(
            "/runs/{run}/tasks/{task}/logs?follow={follow}{attempt}"
        ))
        .await?;
    let mut body = response.bytes_stream();
    let mut pending = Vec::new();
    let mut out = std::io::stdout();
    while let Some(piece) = body.next().await {
        pending.extend_from_slice(&piece?);
        // Print whole lines; keep the unfinished one for the next piece.
        while let Some(end) = pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = pending.drain(..=end).collect();
            let line = String::from_utf8_lossy(&line[..end]);
            writeln!(out, "{}", LogLine::parse(&line).display())?;
        }
        out.flush()?;
    }
    Ok(())
}

/// `lwwo workers`.
pub(crate) async fn workers(client: &Client) -> Result<()> {
    let workers = client.get("/workers").await?;
    let rows: Vec<Vec<String>> = workers
        .as_array()
        .into_iter()
        .flatten()
        .map(|worker| {
            let list = |field: &str| {
                let items: Vec<String> = worker[field]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(text)
                    .collect();
                items.join(",")
            };
            let state = if worker["accepting"] == json!(true) {
                "ready"
            } else {
                "not accepting"
            };
            vec![
                text(&worker["name"]),
                format!("{}/{}", text(&worker["used"]), text(&worker["slots"])),
                state.to_owned(),
                list("labels"),
            ]
        })
        .collect();
    table(&["WORKER", "SLOTS", "STATE", "LABELS"], &rows);
    Ok(())
}

/// `lwwo token list`.
pub(crate) async fn tokens(client: &Client) -> Result<()> {
    let tokens = client.get("/tokens").await?;
    let rows: Vec<Vec<String>> = tokens
        .as_array()
        .into_iter()
        .flatten()
        .map(|token| {
            let kind = if token["kind"] == "join" {
                "worker"
            } else {
                "api"
            };
            vec![
                text(&token["id"]),
                kind.to_owned(),
                text(&token["state"]),
                time(&token["created_at"]),
                time(&token["expires_at"]),
                text(&token["label"]),
            ]
        })
        .collect();
    table(
        &["ID", "KIND", "STATE", "CREATED", "EXPIRES", "LABEL"],
        &rows,
    );
    Ok(())
}

/// `lwwo status`.
pub(crate) async fn status(client: &Client) -> Result<()> {
    let status = client.get("/status").await?;
    println!(
        "master {} (lwwo {})\nworkers {}  slots {}/{}  active runs {}  queued {}  DAGs {}\nnext run: {}",
        client.address,
        text(&status["version"]),
        text(&status["workers"]),
        text(&status["slots_used"]),
        text(&status["slots"]),
        text(&status["active_runs"]),
        text(&status["queued_runs"]),
        text(&status["dags"]),
        time(&status["next_fire"]),
    );
    Ok(())
}

/// A command whose answer is one line: prints `field` of the answer.
pub(crate) fn done(answer: &Value, fields: &[&str]) {
    let line: Vec<String> = fields.iter().map(|field| text(&answer[*field])).collect();
    println!("{}", line.join(" "));
}
