//! Reading and checking the YAML of a DAG.
//!
//! [`Dag::parse`] turns the text given to `lwwo apply` into a [`Dag`] with
//! `defaults` already merged into every task, and rejects what cannot run:
//! unknown fields, bad names, cycles, templates that name unknown values.
//! The same text always gives the same [`Dag::hash`], whatever its spacing
//! or comments, which is what identifies a version.

mod template;

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use lwwo_core::cron::Cron;
use lwwo_core::dag::{Backoff, Graph, TaskDef, TaskOptions};
use lwwo_core::timetable::{Overlap, Schedule};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{BackoffKind, Defaults, OverlapKind};
use crate::util::{parse_duration, sha256_hex};
use crate::zone::Zone;

pub(crate) use template::RunContext;

/// A task as written: every field optional, so that the same shape reads
/// both `defaults` and the tasks.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTask {
    run: Option<Vec<String>>,
    needs: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    labels: Option<Vec<String>>,
    slots: Option<u32>,
    retries: Option<u32>,
    retry_delay: Option<String>,
    retry_backoff: Option<BackoffKind>,
    max_retry_delay: Option<String>,
    timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDag {
    name: String,
    schedule: Option<String>,
    timezone: Option<String>,
    catchup: Option<bool>,
    overlap: Option<OverlapKind>,
    max_active: Option<u32>,
    #[serde(default)]
    params: BTreeMap<String, Value>,
    #[serde(default)]
    defaults: RawTask,
    tasks: BTreeMap<String, RawTask>,
}

/// A task of a DAG, with the DAG's `defaults` merged in. What is still
/// `None` falls back to the `[defaults]` of the master.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Task {
    pub(crate) name: String,
    pub(crate) run: Vec<String>,
    pub(crate) needs: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) labels: Vec<String>,
    pub(crate) slots: u32,
    pub(crate) retries: Option<u32>,
    pub(crate) retry_delay_ms: Option<u64>,
    pub(crate) retry_backoff: Option<BackoffKind>,
    pub(crate) max_retry_delay_ms: Option<u64>,
    pub(crate) timeout_ms: Option<u64>,
}

/// A DAG as applied. Tasks are in name order, which is also the order of
/// their [`TaskId`](lwwo_core::dag::TaskId)s in the graph.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Dag {
    pub(crate) name: String,
    pub(crate) schedule: Option<String>,
    pub(crate) timezone: Option<String>,
    pub(crate) catchup: Option<bool>,
    pub(crate) overlap: Option<OverlapKind>,
    pub(crate) max_active: u32,
    pub(crate) params: BTreeMap<String, Value>,
    pub(crate) tasks: Vec<Task>,
}

/// Letters, digits, `_` and `-`: names end up in paths and in URLs.
fn check_name(kind: &str, name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "the {kind} name is empty");
    ensure!(
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "the {kind} name `{name}` may only have letters, digits, `_` and `-`"
    );
    Ok(())
}

fn millis(field: &str, text: Option<&String>) -> Result<Option<u64>> {
    text.map(|text| {
        let duration = parse_duration(text).with_context(|| format!("in `{field}`"))?;
        Ok(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
    })
    .transpose()
}

impl Task {
    fn merge(name: &str, task: RawTask, defaults: &RawTask) -> Result<Self> {
        let run = task
            .run
            .with_context(|| format!("task `{name}` has no `run`"))?;
        ensure!(!run.is_empty(), "task `{name}` has an empty `run`");
        let mut env = defaults.env.clone().unwrap_or_default();
        env.extend(task.env.unwrap_or_default());
        for key in env.keys() {
            ensure!(
                !key.starts_with("LWWO_"),
                "task `{name}` sets `{key}`: names starting with LWWO_ are reserved"
            );
        }
        let slots = task.slots.or(defaults.slots).unwrap_or(1);
        Ok(Self {
            name: name.to_owned(),
            run,
            needs: task.needs.unwrap_or_default(),
            env,
            labels: task
                .labels
                .or_else(|| defaults.labels.clone())
                .unwrap_or_default(),
            slots,
            retries: task.retries.or(defaults.retries),
            retry_delay_ms: millis(
                "retry_delay",
                task.retry_delay.as_ref().or(defaults.retry_delay.as_ref()),
            )?,
            retry_backoff: task.retry_backoff.or(defaults.retry_backoff),
            max_retry_delay_ms: millis(
                "max_retry_delay",
                task.max_retry_delay
                    .as_ref()
                    .or(defaults.max_retry_delay.as_ref()),
            )?,
            timeout_ms: millis(
                "timeout",
                task.timeout.as_ref().or(defaults.timeout.as_ref()),
            )?,
        })
    }

    /// Placement, retries and timeout for the core, with the master's
    /// defaults where the DAG says nothing.
    fn options(&self, defaults: &Defaults) -> TaskOptions {
        let backoff = match self.retry_backoff.unwrap_or(defaults.retry_backoff) {
            BackoffKind::Exponential => Backoff::Exponential,
            BackoffKind::Fixed => Backoff::Fixed,
        };
        TaskOptions {
            slots: self.slots,
            labels: self.labels.clone(),
            retries: self.retries.unwrap_or(defaults.retries),
            retry_delay: self
                .retry_delay_ms
                .map_or(defaults.retry_delay.0, Duration::from_millis),
            retry_backoff: backoff,
            max_retry_delay: self
                .max_retry_delay_ms
                .map(Duration::from_millis)
                .or(defaults.max_retry_delay.map(|d| d.0)),
            timeout: self
                .timeout_ms
                .map(Duration::from_millis)
                .or(defaults.timeout.map(|d| d.0)),
        }
    }
}

impl Dag {
    /// Reads and checks the YAML of a DAG.
    pub(crate) fn parse(yaml: &str) -> Result<Self> {
        let raw: RawDag = serde_yaml_ng::from_str(yaml).context("invalid DAG")?;
        check_name("DAG", &raw.name)?;
        ensure!(!raw.tasks.is_empty(), "DAG `{}` has no tasks", raw.name);
        ensure!(
            raw.defaults.run.is_none() && raw.defaults.needs.is_none(),
            "`run` and `needs` cannot be in `defaults`"
        );
        for (name, value) in &raw.params {
            check_name("parameter", name)?;
            ensure!(
                value.is_string() || value.is_number() || value.is_boolean(),
                "the default of parameter `{name}` must be a text, a number or true/false"
            );
        }
        let mut tasks = Vec::with_capacity(raw.tasks.len());
        for (name, task) in raw.tasks {
            check_name("task", &name)?;
            tasks.push(Task::merge(&name, task, &raw.defaults)?);
        }
        let dag = Self {
            name: raw.name,
            schedule: raw.schedule,
            timezone: raw.timezone,
            catchup: raw.catchup,
            overlap: raw.overlap,
            max_active: raw.max_active.unwrap_or(10),
            params: raw.params,
            tasks,
        };
        ensure!(dag.max_active > 0, "`max_active` must be at least 1");

        // Everything that would only fail later, when a run starts.
        if let Some(schedule) = &dag.schedule {
            schedule
                .parse::<Cron>()
                .with_context(|| format!("invalid schedule `{schedule}`"))?;
        }
        if let Some(zone) = &dag.timezone {
            Zone::named(zone)?;
        }
        dag.graph(&Defaults::default())?;
        template::check(&dag)?;
        Ok(dag)
    }

    /// The hash that identifies this version: of the definition as
    /// understood, so spacing and comments do not change it.
    pub(crate) fn hash(&self) -> String {
        let canonical = serde_json::to_vec(self).expect("a DAG is plain data");
        sha256_hex(&canonical)
    }

    /// The hash of tasks and dependencies only: two versions with the same
    /// structure can continue each other's runs.
    pub(crate) fn structure(&self) -> String {
        let shape: Vec<(&str, Vec<&str>)> = self
            .tasks
            .iter()
            .map(|task| {
                let mut needs: Vec<&str> = task.needs.iter().map(String::as_str).collect();
                needs.sort_unstable();
                (task.name.as_str(), needs)
            })
            .collect();
        sha256_hex(&serde_json::to_vec(&shape).expect("names are plain data"))
    }

    /// The graph of the DAG for the engine.
    pub(crate) fn graph(&self, defaults: &Defaults) -> Result<Graph> {
        let tasks: Vec<TaskDef> = self
            .tasks
            .iter()
            .map(|task| TaskDef {
                name: task.name.clone(),
                needs: task.needs.clone(),
                options: task.options(defaults),
            })
            .collect();
        Graph::build(&tasks).with_context(|| format!("invalid DAG `{}`", self.name))
    }

    /// The time zone of the DAG.
    pub(crate) fn zone(&self, default_timezone: &str) -> Result<Zone> {
        Zone::named(self.timezone.as_deref().unwrap_or(default_timezone))
    }

    /// The overlap rule of the DAG.
    pub(crate) fn overlap(&self, defaults: &Defaults) -> Overlap {
        match self.overlap.unwrap_or(defaults.overlap) {
            OverlapKind::Queue => Overlap::Queue,
            OverlapKind::Skip => Overlap::Skip,
            OverlapKind::Allow => Overlap::Allow {
                max_active: self.max_active,
            },
            OverlapKind::Replace => Overlap::Replace,
        }
    }

    /// The schedule of the DAG, or `None` if it only runs by hand.
    pub(crate) fn schedule(
        &self,
        defaults: &Defaults,
        default_timezone: &str,
    ) -> Result<Option<Schedule<Zone>>> {
        let Some(schedule) = &self.schedule else {
            return Ok(None);
        };
        let cron: Cron = schedule
            .parse()
            .with_context(|| format!("invalid schedule `{schedule}`"))?;
        Ok(Some(Schedule::new(
            cron,
            self.zone(default_timezone)?,
            self.catchup.unwrap_or(defaults.catchup),
            self.overlap(defaults),
        )))
    }

    /// The parameters of a run: the declared defaults, with `overrides`
    /// on top. An unknown name or a value of the wrong type is rejected.
    pub(crate) fn resolve_params(
        &self,
        overrides: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, Value>> {
        let mut params = self.params.clone();
        for (name, text) in overrides {
            let Some(default) = self.params.get(name) else {
                let known: Vec<&str> = self.params.keys().map(String::as_str).collect();
                bail!(
                    "DAG `{}` has no parameter `{name}`; it declares: {}",
                    self.name,
                    if known.is_empty() {
                        "none".to_owned()
                    } else {
                        known.join(", ")
                    }
                );
            };
            // The type of a parameter is the type of its default.
            let value = match default {
                Value::Bool(_) => Value::Bool(text.parse().with_context(|| {
                    format!("parameter `{name}` must be true or false, not `{text}`")
                })?),
                Value::Number(number) if number.is_f64() => {
                    let float: f64 = text.parse().with_context(|| {
                        format!("parameter `{name}` must be a number, not `{text}`")
                    })?;
                    serde_json::Number::from_f64(float)
                        .map(Value::Number)
                        .with_context(|| format!("parameter `{name}` must be a finite number"))?
                }
                Value::Number(_) => Value::Number(
                    text.parse::<i64>()
                        .with_context(|| {
                            format!("parameter `{name}` must be a whole number, not `{text}`")
                        })?
                        .into(),
                ),
                _ => Value::String(text.clone()),
            };
            params.insert(name.clone(), value);
        }
        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETL: &str = r#"
name: etl_sales
schedule: "0 3 * * *"
timezone: Europe/Rome
params:
  country: IT
  threshold: 100
defaults:
  retries: 2
  timeout: 1h
tasks:
  extract:
    run: ["python", "extract.py", "--country", "{{ params.country }}"]
  clean:
    needs: [extract]
    run: ["python", "clean.py", "--day", "{{ run.date }}"]
  load:
    needs: [clean]
    run: ["python", "load.py"]
    labels: [big]
    slots: 2
    timeout: 3h
    retries: 0
"#;

    fn error(yaml: &str) -> String {
        format!("{:#}", Dag::parse(yaml).unwrap_err())
    }

    #[test]
    fn reads_a_dag_and_merges_the_defaults() {
        let dag = Dag::parse(ETL).unwrap();
        assert_eq!(dag.name, "etl_sales");
        let names: Vec<&str> = dag.tasks.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["clean", "extract", "load"], "in name order");
        assert_eq!(dag.tasks[0].retries, Some(2));
        assert_eq!(dag.tasks[0].timeout_ms, Some(3_600_000));
        assert_eq!(dag.tasks[2].retries, Some(0));
        assert_eq!(dag.tasks[2].timeout_ms, Some(10_800_000));
        assert_eq!(dag.tasks[2].slots, 2);

        let graph = dag.graph(&Defaults::default()).unwrap();
        assert_eq!(graph.len(), 3);
        assert!(dag.schedule(&Defaults::default(), "UTC").unwrap().is_some());
    }

    #[test]
    fn the_hash_ignores_comments_and_follows_the_content() {
        let dag = Dag::parse(ETL).unwrap();
        let commented = Dag::parse(&format!("# nightly\n{ETL}\n")).unwrap();
        assert_eq!(dag.hash(), commented.hash());

        let changed = Dag::parse(&ETL.replace("load.py", "load2.py")).unwrap();
        assert_ne!(dag.hash(), changed.hash());
        assert_eq!(dag.structure(), changed.structure(), "same tasks and needs");

        let reshaped = Dag::parse(&ETL.replace("needs: [clean]", "needs: [extract]")).unwrap();
        assert_ne!(dag.structure(), reshaped.structure());
    }

    #[test]
    fn rejects_what_cannot_run() {
        assert!(error("name: a\ntasks: {}").contains("no tasks"));
        assert!(error("name: a b\ntasks: {t: {run: [x]}}").contains("letters"));
        assert!(error("name: a\ntasks: {t: {}}").contains("no `run`"));
        assert!(error("name: a\ntasks: {t: {run: [x], need: [u]}}").contains("unknown field"));
        assert!(error("name: a\ntasks: {t: {run: [x], needs: [u]}}").contains('u'));
        assert!(
            error("name: a\ntasks: {t: {run: [x], needs: [u]}, u: {run: [x], needs: [t]}}")
                .contains("cycle")
        );
        assert!(error("name: a\nschedule: nightly\ntasks: {t: {run: [x]}}").contains("schedule"));
        assert!(error("name: a\ntimezone: Mars/Olympus\ntasks: {t: {run: [x]}}").contains("zone"));
        assert!(error("name: a\ntasks: {t: {run: [x], timeout: soon}}").contains("timeout"));
        assert!(
            error("name: a\ntasks: {t: {run: ['{{ params.missing }}']}}").contains("missing"),
            "a template that names an undeclared parameter"
        );
        assert!(error("name: a\ntasks: {t: {run: [x], env: {LWWO_X: y}}}").contains("reserved"));
    }

    #[test]
    fn the_examples_are_valid() {
        let dag = Dag::parse(include_str!("../../../../examples/dags/etl_sales.yaml")).unwrap();
        dag.graph(&Defaults::default()).unwrap();
    }

    #[test]
    fn parameters_take_the_type_of_their_default() {
        let dag = Dag::parse(ETL).unwrap();
        let set = |name: &str, value: &str| {
            dag.resolve_params(&BTreeMap::from([(name.to_owned(), value.to_owned())]))
        };
        let params = set("country", "FR").unwrap();
        assert_eq!(params["country"], "FR");
        assert_eq!(params["threshold"], 100, "the others keep their defaults");
        assert_eq!(set("threshold", "250").unwrap()["threshold"], 250);
        assert!(set("threshold", "abc").is_err());
        assert!(set("contry", "FR").is_err(), "a typo does not go unnoticed");
    }
}
