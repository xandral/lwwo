//! The templates in a task's command and environment.
//!
//! They use Jinja syntax and hold values only: the identity of the run and
//! its parameters. The same values reach the task as `LWWO_*` variables.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use minijinja::{Environment, UndefinedBehavior};
use serde_json::{Value, json};

use super::{Dag, Task};

/// What a template can name: one attempt of one task of one run.
#[derive(Debug, Clone)]
pub(crate) struct RunContext<'a> {
    pub(crate) dag: &'a str,
    pub(crate) run_id: &'a str,
    /// The instant of the run, as RFC 3339 in the DAG's time zone.
    pub(crate) run_time: &'a str,
    /// The day of the run in the DAG's time zone.
    pub(crate) run_date: &'a str,
    pub(crate) params: &'a BTreeMap<String, Value>,
}

/// The command and the environment of one attempt, ready for the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rendered {
    pub(crate) command: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
}

fn environment() -> Environment<'static> {
    let mut env = Environment::new();
    // A misspelled name is an error, not an empty string.
    env.set_undefined_behavior(UndefinedBehavior::Strict);
    env
}

/// A parameter as text, for an environment variable.
fn plain(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

impl RunContext<'_> {
    /// Renders the command and the environment of `task` for `attempt`,
    /// and adds the `LWWO_*` variables.
    pub(crate) fn render(&self, task: &Task, attempt: u32) -> Result<Rendered> {
        let values = json!({
            "dag": { "name": self.dag },
            "run": { "id": self.run_id, "time": self.run_time, "date": self.run_date },
            "task": { "name": task.name, "attempt": attempt },
            "params": self.params,
        });
        let jinja = environment();
        let render = |text: &str| {
            jinja
                .render_str(text, &values)
                .with_context(|| format!("in the template `{text}` of task `{}`", task.name))
        };

        let command = task
            .run
            .iter()
            .map(|arg| render(arg))
            .collect::<Result<_>>()?;
        let mut env = BTreeMap::new();
        for (name, value) in &task.env {
            env.insert(name.clone(), render(value)?);
        }
        env.insert("LWWO_DAG".to_owned(), self.dag.to_owned());
        env.insert("LWWO_RUN_ID".to_owned(), self.run_id.to_owned());
        env.insert("LWWO_RUN_TIME".to_owned(), self.run_time.to_owned());
        env.insert("LWWO_RUN_DATE".to_owned(), self.run_date.to_owned());
        env.insert("LWWO_TASK".to_owned(), task.name.clone());
        env.insert("LWWO_ATTEMPT".to_owned(), attempt.to_string());
        for (name, value) in self.params {
            env.insert(
                format!("LWWO_PARAM_{}", name.to_ascii_uppercase().replace('-', "_")),
                plain(value),
            );
        }
        Ok(Rendered { command, env })
    }
}

/// Renders every template of `dag` once with made-up run values, so that a
/// bad template is rejected by `lwwo apply` instead of failing a run.
pub(super) fn check(dag: &Dag) -> Result<()> {
    let context = RunContext {
        dag: &dag.name,
        run_id: "00000000-0000-7000-8000-000000000000",
        run_time: "1970-01-01T00:00:00+00:00",
        run_date: "1970-01-01",
        params: &dag.params,
    };
    for task in &dag.tasks {
        context.render(task, 1)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_command_and_the_variables() {
        let dag = Dag::parse(
            r#"
name: sales
params: {country: IT, threshold: 100}
tasks:
  clean:
    run: ["clean", "--day", "{{ run.date }}", "--to", "s3://data/{{ run.date }}/{{ params.country }}.parquet"]
    env:
      ATTEMPT_DIR: "/tmp/{{ dag.name }}/{{ task.name }}/{{ task.attempt }}"
"#,
        )
        .unwrap();
        let context = RunContext {
            dag: "sales",
            run_id: "0192f3a4",
            run_time: "2026-10-26T03:00:00+01:00",
            run_date: "2026-10-26",
            params: &dag.params,
        };
        let rendered = context.render(&dag.tasks[0], 2).unwrap();
        assert_eq!(
            rendered.command,
            [
                "clean",
                "--day",
                "2026-10-26",
                "--to",
                "s3://data/2026-10-26/IT.parquet"
            ]
        );
        assert_eq!(rendered.env["ATTEMPT_DIR"], "/tmp/sales/clean/2");
        assert_eq!(rendered.env["LWWO_DAG"], "sales");
        assert_eq!(rendered.env["LWWO_RUN_ID"], "0192f3a4");
        assert_eq!(rendered.env["LWWO_RUN_TIME"], "2026-10-26T03:00:00+01:00");
        assert_eq!(rendered.env["LWWO_TASK"], "clean");
        assert_eq!(rendered.env["LWWO_ATTEMPT"], "2");
        assert_eq!(rendered.env["LWWO_PARAM_COUNTRY"], "IT");
        assert_eq!(rendered.env["LWWO_PARAM_THRESHOLD"], "100");
    }
}
