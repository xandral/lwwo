//! lwwo, the Lightweight Workflow Orchestrator.
//!
//! One binary that runs as master, as worker, as both in one process
//! (`standalone`), as terminal interface (`tui`) or as command line client.
//! The decisions are taken by the `lwwo-core` crate, which does no I/O;
//! everything here carries them out: SQLite, gRPC, HTTP, processes.

mod cli;
mod client;
mod config;
mod dsl;
mod master;
mod proto;
mod security;
mod store;
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    reason = "screen coordinates are small numbers; drawing functions read top to bottom"
)]
mod tui;
mod util;
mod worker;
mod zone;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use serde_json::json;
use tracing::error;

use crate::client::Client;

#[derive(Debug, Parser)]
#[command(name = "lwwo", version, about = "Lightweight Workflow Orchestrator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The configuration file, and any field of it as a flag.
#[derive(Debug, Args)]
struct Settings {
    /// TOML configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Any field of the configuration, as `--section-field value`.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "OPTIONS"
    )]
    options: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the master.
    Master(Settings),
    /// Start a worker, or remove one from the master.
    #[command(args_conflicts_with_subcommands = true)]
    Worker {
        #[command(subcommand)]
        action: Option<WorkerAction>,
        #[command(flatten)]
        settings: Settings,
    },
    /// Master and worker in one process, with data in ./lwwo-data.
    Standalone(Settings),
    /// Validate a DAG and save it as the current version.
    Apply {
        /// The YAML file of the DAG.
        file: PathBuf,
    },
    /// Start a run by hand.
    Run {
        dag: String,
        /// The instant of the run, such as 2026-10-01T03:00.
        #[arg(long)]
        at: Option<String>,
        /// A parameter declared in the DAG, as name=value.
        #[arg(long = "param", value_name = "NAME=VALUE")]
        params: Vec<String>,
    },
    /// Open the terminal interface.
    Tui {
        /// Show made-up data, without a master: to look around.
        #[arg(long)]
        demo: bool,
    },
    /// Create or print tokens.
    Token {
        #[command(subcommand)]
        kind: TokenCommand,
    },
    /// List the DAGs.
    Dags,
    /// List the latest runs.
    Runs {
        #[arg(long)]
        dag: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    /// Show a run with its tasks.
    Show { run: String },
    /// Print the log of a task of a run.
    Logs {
        run: String,
        task: String,
        /// Which attempt; the latest if not given.
        #[arg(long)]
        attempt: Option<u32>,
        /// Keep printing while the task runs.
        #[arg(short, long)]
        follow: bool,
    },
    /// Cancel a run.
    Cancel { run: String },
    /// Run again the failed tasks of a finished run, or more.
    Rerun {
        run: String,
        /// Only this task and what depends on it.
        #[arg(long)]
        task: Option<String>,
        /// With --task: not what depends on it.
        #[arg(long, requires = "task")]
        only: bool,
        /// Every task of the run.
        #[arg(long, conflicts_with = "task")]
        all: bool,
        /// Use the current version of the DAG, if its tasks are the same.
        #[arg(long)]
        current: bool,
    },
    /// Stop the schedule of a DAG.
    Pause { dag: String },
    /// Start the schedule of a paused DAG again.
    Resume { dag: String },
    /// Remove a DAG; its runs stay in the history.
    Delete { dag: String },
    /// List the connected workers.
    Workers,
    /// Tell a worker to finish what it has and take nothing more.
    Drain { worker: String },
    /// Show the state of the master.
    Status,
    /// Copy the database, while the master runs, into the master's backup
    /// directory.
    Backup {
        /// A name for the copy; one with the time in it if not given.
        name: Option<String>,
    },
    /// Watch over one task: started by a worker, not by people.
    #[command(hide = true)]
    Shim {
        /// The journal directory of the attempt.
        dir: PathBuf,
    },
    /// Call the API directly and print the JSON answer, for scripts.
    Api {
        /// GET, POST or DELETE.
        method: String,
        /// Such as /runs?dag=etl.
        path: String,
        /// JSON body of a POST.
        body: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerAction {
    /// Remove a worker and revoke its certificate.
    Remove { name: String },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    /// Print the join token for workers.
    Worker {
        /// Replace it: the old one stops working.
        #[arg(long)]
        rotate: bool,
    },
    /// Create a token for the CLI and the TUI.
    Api {
        /// A name to recognise it by.
        #[arg(long)]
        label: Option<String>,
        /// It stops working after this long, such as 90d.
        #[arg(long)]
        expires: Option<String>,
    },
    /// List the tokens: id, kind, name, and whether they still work.
    List,
    /// Revoke a token: it stops working at once.
    Revoke {
        /// The id, as `lwwo token list` shows it.
        id: String,
    },
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Waits until the process is asked to stop: Ctrl-C, or the SIGTERM that
/// `systemctl stop` and `docker stop` send.
async fn stop_requested() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    let terminated = async {
        match &mut terminate {
            Ok(signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminated => {}
    }
}

/// Waits for the master to stop, or to be asked to.
async fn until_stopped(done: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    tokio::select! {
        stopped = done => stopped.context("the master crashed")?,
        () = stop_requested() => Ok(()),
    }
}

async fn serve(command: Command) -> Result<()> {
    match command {
        Command::Master(settings) => {
            let (config, _) =
                config::load_master(settings.config.as_deref(), &settings.options, false, &env)?;
            let secure = true;
            until_stopped(master::start(config, secure).await?.done).await
        }
        Command::Standalone(settings) => {
            let (mut config, worker) =
                config::load_master(settings.config.as_deref(), &settings.options, true, &env)?;
            // Master and worker talk inside this process, without TLS:
            // nobody else may connect.
            config.workers.listen = ([127, 0, 0, 1], 0).into();
            let running = master::start(config, false).await?;
            let address = running.workers.to_string();
            tokio::spawn(async move {
                if let Err(error) = worker::run(worker, address, false).await {
                    error!("the worker stopped: {error:#}");
                }
            });
            until_stopped(running.done).await
        }
        Command::Worker { settings, .. } => {
            let config = config::load_worker(settings.config.as_deref(), &settings.options, &env)?;
            let address = config.master.clone().context("no master address")?;
            tokio::select! {
                stopped = worker::run(config, address, true) => stopped,
                // The worker just leaves: its tasks go on under their
                // shims, and the next worker picks them up.
                () = stop_requested() => Ok(()),
            }
        }
        _ => unreachable!("only the long-running commands come here"),
    }
}

#[expect(clippy::too_many_lines, reason = "one arm per command")]
async fn ask(command: Command) -> Result<()> {
    let client = Client::connect(&config::load_client(&env)?).await?;
    match command {
        Command::Apply { file } => cli::apply(&client, &file).await,
        Command::Run { dag, at, params } => cli::run(&client, &dag, at.as_deref(), &params).await,
        Command::Tui { .. } => tui::run(client).await,
        Command::Token { kind } => {
            let body = match kind {
                TokenCommand::Worker { rotate } => json!({ "kind": "worker", "rotate": rotate }),
                TokenCommand::Api { label, expires } => {
                    json!({ "kind": "api", "label": label, "expires": expires })
                }
                TokenCommand::List => return cli::tokens(&client).await,
                TokenCommand::Revoke { id } => {
                    client.delete(&format!("/tokens/{id}")).await?;
                    println!("{id} revoked");
                    return Ok(());
                }
            };
            cli::done(&client.post("/tokens", &body).await?, &["token"]);
            Ok(())
        }
        Command::Dags => cli::dags(&client).await,
        Command::Runs { dag, limit } => cli::runs(&client, dag.as_deref(), limit).await,
        Command::Show { run } => cli::show(&client, &run).await,
        Command::Logs {
            run,
            task,
            attempt,
            follow,
        } => cli::logs(&client, &run, &task, attempt, follow).await,
        Command::Cancel { run } => {
            let answer = client
                .post(&format!("/runs/{run}/cancel"), &json!({}))
                .await?;
            cli::done(&answer, &["id", "state"]);
            Ok(())
        }
        Command::Rerun {
            run,
            task,
            only,
            all,
            current,
        } => {
            let body = json!({ "task": task, "only": only, "all": all, "current": current });
            let answer = client.post(&format!("/runs/{run}/rerun"), &body).await?;
            cli::done(&answer, &["id", "state"]);
            Ok(())
        }
        Command::Pause { dag } => {
            client
                .post(&format!("/dags/{dag}/pause"), &json!({}))
                .await?;
            println!("{dag} paused");
            Ok(())
        }
        Command::Resume { dag } => {
            let answer = client
                .post(&format!("/dags/{dag}/resume"), &json!({}))
                .await?;
            match answer["next_fire"].as_i64() {
                Some(next) => println!("{dag} resumed: next run {}", util::format_local(next)),
                None => println!("{dag} resumed"),
            }
            Ok(())
        }
        Command::Delete { dag } => {
            client.delete(&format!("/dags/{dag}")).await?;
            println!("{dag} deleted");
            Ok(())
        }
        Command::Workers => cli::workers(&client).await,
        Command::Drain { worker } => {
            client
                .post(&format!("/workers/{worker}/drain"), &json!({}))
                .await?;
            println!("{worker} is draining");
            Ok(())
        }
        Command::Worker {
            action: Some(WorkerAction::Remove { name }),
            ..
        } => {
            client.delete(&format!("/workers/{name}")).await?;
            println!("{name} removed");
            Ok(())
        }
        Command::Status => cli::status(&client).await,
        Command::Backup { name } => {
            let answer = client.post("/backup", &json!({ "name": name })).await?;
            println!(
                "{} written on the master: {} bytes",
                answer["path"].as_str().unwrap_or_default(),
                answer["bytes"]
            );
            Ok(())
        }
        Command::Api { method, path, body } => {
            let answer = match method.to_ascii_uppercase().as_str() {
                "GET" => client.get(&path).await?,
                "DELETE" => client.delete(&path).await?,
                "POST" => {
                    let body = body.as_deref().unwrap_or("{}");
                    let body = serde_json::from_str(body).context("the body is not JSON")?;
                    client.post(&path, &body).await?
                }
                other => anyhow::bail!("unknown method `{other}`: use GET, POST or DELETE"),
            };
            println!("{}", serde_json::to_string_pretty(&answer)?);
            Ok(())
        }
        Command::Master(_)
        | Command::Standalone(_)
        | Command::Worker { .. }
        | Command::Shim { .. } => {
            unreachable!("the long-running commands do not come here")
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    security::install_crypto();
    let serving = matches!(
        cli.command,
        Command::Master(_) | Command::Standalone(_) | Command::Worker { action: None, .. }
    );
    if serving {
        let level = env("LWWO_LOG").unwrap_or_else(|| "info".to_owned());
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(level))
            .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
            .with_writer(std::io::stderr)
            .init();
    }
    let result = if let Command::Shim { dir } = &cli.command {
        worker::shim::run(dir).await
    } else if matches!(cli.command, Command::Tui { demo: true }) {
        tui::run_demo()
    } else if serving {
        serve(cli.command).await
    } else {
        ask(cli.command).await
    };
    if let Err(error) = result {
        eprintln!("lwwo: {error:#}");
        std::process::exit(1);
    }
}
