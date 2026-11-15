//! The state of the interface and how keys and time change it.
//!
//! Nothing here draws or reads the terminal: `ui` turns an [`App`] into a
//! picture, and `mod` feeds it keys and the clock. That split is what lets
//! the tests press keys and inspect the result without a terminal.

use std::cell::{Cell, OnceCell};

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::backend::{
    Backend, DagRow, Rerun, Result, RunRow, RunState, RunView, Status, TaskRow, TaskState,
    WorkerRow,
};
use super::dialog::Dialog;
use super::graph::Picture;
use super::theme;
use crate::client::LogLine;

/// How often the data on screen is read again when the master is quiet
/// (ms). When something changes the master says so, and the screen is read
/// at once.
const REFRESH_EVERY: i64 = 2_000;
/// How often an open log asks for new lines (ms).
const LOG_EVERY: i64 = 300;
/// How many runs the list shows.
const RUN_LIMIT: usize = 200;
/// Columns moved by one press of left or right in a log.
const SIDE_STEP: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Screen {
    Runs,
    Dags,
    Workers,
    /// One run: its levels and its tasks.
    Run,
    /// One run, or one DAG: its graph with every dependency drawn.
    Graph,
    /// The output of one task, or of all the tasks of a run.
    Log,
}

/// Which lines of a log are shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Show {
    All,
    /// Standard output, and what lwwo says.
    Out,
    /// Standard error only.
    Err,
}

impl Show {
    fn next(self) -> Self {
        match self {
            Self::All => Self::Out,
            Self::Out => Self::Err,
            Self::Err => Self::All,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Out => "stdout",
            Self::Err => "stderr",
        }
    }

    pub(crate) fn takes(self, line: &LogLine) -> bool {
        match self {
            Self::All => true,
            Self::Out => line.stream != 'E',
            Self::Err => line.stream == 'E',
        }
    }
}

pub(crate) struct App<B> {
    backend: B,
    pub(crate) screen: Screen,
    pub(crate) status: Status,
    pub(crate) runs: Vec<RunRow>,
    pub(crate) run_cursor: usize,
    /// Only the runs of this DAG, when the list was opened from a DAG.
    pub(crate) only_dag: Option<String>,
    pub(crate) dags: Vec<DagRow>,
    pub(crate) dag_cursor: usize,
    pub(crate) workers: Vec<WorkerRow>,
    pub(crate) worker_cursor: usize,
    /// The run on display in the `Run`, `Graph` and `Log` screens.
    pub(crate) detail: Option<Detail>,
    pub(crate) log: Option<LogView>,
    pub(crate) dialog: Dialog,
    /// Why the data on screen could not be refreshed, while that lasts.
    pub(crate) problem: Option<String>,
    pub(crate) confirm: Option<Confirm>,
    /// Text being typed in the message box: a filter, a command, parameters.
    pub(crate) prompt: Option<(Prompt, String)>,
    /// Rows of the lists, or lines of the log, must contain this.
    pub(crate) filter: String,
    pub(crate) help: bool,
    /// Current time (unix ms), updated at every tick.
    pub(crate) now: i64,
    refreshed_at: i64,
    log_read_at: i64,
    /// The runs were loaded at least once: changes after that are news.
    loaded: bool,
    /// Rows of the main list in the last drawing, for page up and page down.
    /// The drawing code sets it; a `Cell` lets it do so through `&App`.
    pub(crate) page_rows: Cell<usize>,
    /// First visible row of each list, also kept up to date by the drawing.
    pub(crate) runs_offset: Cell<usize>,
    pub(crate) dags_offset: Cell<usize>,
    pub(crate) workers_offset: Cell<usize>,
    pub(crate) tasks_offset: Cell<usize>,
    /// First visible cell and row of the picture in the `Graph` screen.
    pub(crate) graph_offset: Cell<(usize, usize)>,
    /// Where leaving the `Log` screen goes back to.
    log_from: Screen,
    quit: bool,
}

/// A run opened in the `Run` and `Graph` screens, which share the cursor;
/// or a DAG opened in the `Graph` screen without running it.
pub(crate) struct Detail {
    pub(crate) view: RunView,
    /// A DAG seen without running it: there is no run behind `view`.
    pub(crate) preview: bool,
    /// Tasks grouped by depth in the graph, roots first.
    pub(crate) levels: Vec<Vec<usize>>,
    /// Task positions in display order: level by level.
    pub(crate) order: Vec<usize>,
    /// `level_of[task]`: depth of each task in the graph.
    pub(crate) level_of: Vec<usize>,
    /// Position of the cursor in `order`.
    pub(crate) cursor: usize,
    /// The picture of the `Graph` screen, made when first needed: the
    /// tasks of a run and their needs never change.
    picture: OnceCell<Picture>,
    /// The task the cursor left along a line, which going back along a
    /// line prefers.
    came_from: Option<usize>,
}

/// One attempt whose output is part of the log on screen.
struct Source {
    task: String,
    attempt: u32,
    /// Lines read so far.
    read: usize,
}

/// The output of one task, or of every task of a run, in the `Log` screen.
pub(crate) struct LogView {
    /// The task, or `None` for all the tasks of the run together.
    pub(crate) task: Option<String>,
    /// Numbers of the attempts of the task, oldest first.
    pub(crate) attempts: Vec<u32>,
    /// Which of `attempts` is on display.
    pub(crate) attempt: usize,
    sources: Vec<Source>,
    /// The lines, each with the task it comes from.
    pub(crate) lines: Vec<(usize, LogLine)>,
    /// Names of the tasks `lines` refers to.
    pub(crate) names: Vec<String>,
    pub(crate) show: Show,
    /// Lines scrolled back from the end; 0 follows the output as it grows.
    pub(crate) scroll_up: usize,
    pub(crate) scroll_right: usize,
}

/// A yes/no question waiting for an answer.
pub(crate) struct Confirm {
    pub(crate) question: String,
    /// Which answer the cursor is on.
    pub(crate) yes: bool,
    action: Action,
}

/// What a confirmed question does.
enum Action {
    Start(String, Vec<(String, String)>),
    Cancel(String),
    Rerun(String, Rerun),
    Pause(String, bool),
    Drain(String),
}

/// What the message box is being used to type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Prompt {
    /// `/`: keep the rows that contain the text.
    Filter,
    /// `:`: go to a view.
    Command,
    /// `T`: parameters of a run of this DAG, as `name=value name=value`.
    Params(String),
}

impl Detail {
    fn new(view: RunView, preview: bool) -> Self {
        let mut detail = Self {
            levels: Vec::new(),
            view,
            preview,
            order: Vec::new(),
            level_of: Vec::new(),
            cursor: 0,
            picture: OnceCell::new(),
            came_from: None,
        };
        detail.index();
        // Start where the action is: a running task, else a failed one.
        let interesting = |wanted: TaskState| {
            let tasks = &detail.view.tasks;
            detail
                .order
                .iter()
                .position(|&task| tasks[task].state == wanted)
        };
        detail.cursor = interesting(TaskState::Assigned)
            .or_else(|| interesting(TaskState::Failed))
            .unwrap_or(0);
        detail
    }

    /// Computes the levels, the display order and the level of each task.
    fn index(&mut self) {
        self.levels = self.view.levels();
        self.order = self.levels.iter().flatten().copied().collect();
        self.level_of = vec![0; self.view.tasks.len()];
        for (level, tasks) in self.levels.iter().enumerate() {
            for &task in tasks {
                self.level_of[task] = level;
            }
        }
        self.cursor = self.cursor.min(self.order.len().saturating_sub(1));
        // Only a different set of tasks could change the picture.
        if self
            .picture
            .get()
            .is_some_and(|picture| picture.at.len() != self.view.tasks.len())
        {
            self.picture = OnceCell::new();
        }
    }

    pub(crate) fn picture(&self) -> &Picture {
        self.picture
            .get_or_init(|| Picture::new(&self.view.tasks, &self.levels))
    }

    fn select(&mut self, task: usize) {
        if let Some(position) = self.order.iter().position(|&t| t == task) {
            self.cursor = position;
        }
    }

    /// Moves the cursor in the `Graph` screen: up and down its column, or
    /// left and right along a line, to a task it needs or that needs it.
    /// Among several, the one it came from, else the nearest.
    fn walk(&mut self, code: KeyCode, page: usize) {
        let Some(task) = self.selected() else {
            return;
        };
        let picture = self.picture();
        let column = &picture.columns[picture.column_of[task]];
        let here = column.iter().position(|&t| t == task).unwrap_or(0);
        let last = column.len().saturating_sub(1);
        let along = |candidates: &[usize]| {
            if let Some(origin) = self.came_from.filter(|origin| candidates.contains(origin)) {
                return Some(origin);
            }
            let row = picture.at[task].0;
            let distance = |&c: &usize| (picture.at[c].0.abs_diff(row), picture.at[c].0);
            candidates.iter().copied().min_by_key(distance)
        };
        let in_column = |position: usize| column.get(position).copied();
        let (target, along_a_line) = match code {
            KeyCode::Up | KeyCode::Char('k') => (in_column(here.saturating_sub(1)), false),
            KeyCode::Down | KeyCode::Char('j') => (in_column((here + 1).min(last)), false),
            KeyCode::PageUp => (in_column(here.saturating_sub(page)), false),
            KeyCode::PageDown => (in_column((here + page).min(last)), false),
            KeyCode::Home | KeyCode::Char('g') => (in_column(0), false),
            KeyCode::End | KeyCode::Char('G') => (in_column(last), false),
            KeyCode::Left | KeyCode::Char('h') => (along(&picture.needs[task]), true),
            KeyCode::Right | KeyCode::Char('l') => (along(&picture.dependents[task]), true),
            _ => return,
        };
        let Some(target) = target else {
            return;
        };
        self.came_from = along_a_line.then_some(task);
        self.select(target);
    }

    /// Position in `view.tasks` of the task under the cursor.
    pub(crate) fn selected(&self) -> Option<usize> {
        self.order.get(self.cursor).copied()
    }

    pub(crate) fn selected_task(&self) -> Option<&TaskRow> {
        self.selected().map(|task| &self.view.tasks[task])
    }

    /// Moves the cursor to the previous or next level, keeping its row.
    fn move_level(&mut self, forward: bool) {
        let Some(task) = self.selected() else {
            return;
        };
        let level = self.level_of[task];
        let target = if forward {
            level + 1
        } else {
            level.wrapping_sub(1)
        };
        let (Some(current), Some(wanted)) = (self.levels.get(level), self.levels.get(target))
        else {
            return;
        };
        let row = current.iter().position(|&t| t == task).unwrap_or(0);
        let destination = wanted[row.min(wanted.len() - 1)];
        self.select(destination);
    }
}

impl LogView {
    /// The lines that pass the stream choice and the search, in order.
    pub(crate) fn visible<'a>(
        &'a self,
        filter: &'a str,
    ) -> impl Iterator<Item = &'a (usize, LogLine)> {
        let wanted = filter.to_lowercase();
        self.lines.iter().filter(move |(_, line)| {
            self.show.takes(line)
                && (wanted.is_empty() || line.text.to_lowercase().contains(&wanted))
        })
    }
}

/// Whether a row of a list contains the filter, whatever the case.
fn matches(filter: &str, fields: &[&str]) -> bool {
    let wanted = filter.to_lowercase();
    wanted.is_empty()
        || fields
            .iter()
            .any(|field| field.to_lowercase().contains(&wanted))
}

impl<B: Backend> App<B> {
    pub(crate) fn new(backend: B, now: i64) -> Self {
        let mut app = Self {
            backend,
            screen: Screen::Runs,
            status: Status::default(),
            runs: Vec::new(),
            run_cursor: 0,
            only_dag: None,
            dags: Vec::new(),
            dag_cursor: 0,
            workers: Vec::new(),
            worker_cursor: 0,
            detail: None,
            log: None,
            dialog: Dialog::default(),
            problem: None,
            confirm: None,
            prompt: None,
            filter: String::new(),
            help: false,
            now,
            // Makes the first tick load the data.
            refreshed_at: now - REFRESH_EVERY,
            log_read_at: 0,
            loaded: false,
            page_rows: Cell::new(10),
            runs_offset: Cell::new(0),
            dags_offset: Cell::new(0),
            workers_offset: Cell::new(0),
            tasks_offset: Cell::new(0),
            graph_offset: Cell::new((0, 0)),
            log_from: Screen::Run,
            quit: false,
        };
        app.dialog.push("Welcome to LWWO! Press ? for the keys.");
        app
    }

    pub(crate) fn should_quit(&self) -> bool {
        self.quit
    }

    /// True while something on screen is animated.
    pub(crate) fn is_animating(&self) -> bool {
        self.dialog.is_typing()
    }

    // ---- the lists, as filtered --------------------------------------------

    /// The runs that pass the filter, as positions in `runs`.
    pub(crate) fn shown_runs(&self) -> Vec<usize> {
        let keep = |run: &RunRow| {
            let (state, _) = theme::run_state(run.state);
            matches(&self.filter, &[&run.id, &run.dag, state, &run.trigger])
        };
        (0..self.runs.len())
            .filter(|&i| keep(&self.runs[i]))
            .collect()
    }

    pub(crate) fn shown_dags(&self) -> Vec<usize> {
        (0..self.dags.len())
            .filter(|&i| matches(&self.filter, &[&self.dags[i].name]))
            .collect()
    }

    pub(crate) fn shown_workers(&self) -> Vec<usize> {
        let keep = |worker: &WorkerRow| {
            let labels = worker.labels.join(",");
            matches(&self.filter, &[&worker.name, &labels])
        };
        (0..self.workers.len())
            .filter(|&i| keep(&self.workers[i]))
            .collect()
    }

    pub(crate) fn selected_run(&self) -> Option<&RunRow> {
        self.shown_runs()
            .get(self.run_cursor)
            .map(|&i| &self.runs[i])
    }

    pub(crate) fn selected_dag(&self) -> Option<&DagRow> {
        self.shown_dags()
            .get(self.dag_cursor)
            .map(|&i| &self.dags[i])
    }

    pub(crate) fn selected_worker(&self) -> Option<&WorkerRow> {
        self.shown_workers()
            .get(self.worker_cursor)
            .map(|&i| &self.workers[i])
    }

    // ---- time --------------------------------------------------------------

    /// Called regularly with the current time (unix ms).
    pub(crate) fn on_tick(&mut self, now: i64) {
        self.now = now;
        if self.backend.changed() || now - self.refreshed_at >= REFRESH_EVERY {
            self.refresh();
        }
        if self.screen == Screen::Log && now - self.log_read_at >= LOG_EVERY {
            let read = self.read_log();
            self.show(read);
        }
        self.dialog.tick(now);
    }

    /// What the message box shows when it has nothing to announce: a
    /// description of whatever is under the cursor.
    pub(crate) fn hint(&self) -> String {
        match self.screen {
            Screen::Runs => match self.selected_run() {
                None if self.runs.is_empty() => {
                    "No runs yet. Press TAB to pick a DAG and start one.".into()
                }
                None => "No run matches the filter. ESC clears it.".into(),
                Some(run) => {
                    let done = format!("{} of {} tasks done", run.tasks.succeeded, run.tasks.total);
                    let failed = match run.tasks.failed {
                        0 => String::new(),
                        failed => format!(", {failed} failed"),
                    };
                    format!(
                        "{} {}: {done}{failed}. Started by {}, for {}.",
                        run.dag,
                        run.state.word(),
                        run.trigger,
                        theme::when(run.logical_time, self.now),
                    )
                }
            },
            Screen::Dags => match self.selected_dag() {
                None if self.dags.is_empty() => {
                    "No DAGs yet. Give one to lwwo: lwwo apply my-dag.yaml".into()
                }
                None => "No DAG matches the filter. ESC clears it.".into(),
                Some(dag) if dag.paused => {
                    format!(
                        "{} is paused: its schedule is off. p turns it on again.",
                        dag.name
                    )
                }
                Some(dag) => match dag.next_fire {
                    Some(next) => format!(
                        "{} runs next at {}, in {}.",
                        dag.name,
                        theme::when(next, self.now),
                        theme::age(next - self.now)
                    ),
                    None => format!(
                        "{} has no schedule: it runs when you start it with t.",
                        dag.name
                    ),
                },
            },
            Screen::Workers => match self.selected_worker() {
                None => "No worker is connected: runs wait until one joins.".into(),
                Some(worker) if !worker.accepting => format!(
                    "{} takes nothing more: it is draining, or short of disk or memory. It still runs {}.",
                    worker.name,
                    plural(worker.used as usize, "task")
                ),
                Some(worker) => format!(
                    "{} is running {} of the {} it can run at once. Heard from {} ago.",
                    worker.name,
                    plural(worker.used as usize, "task"),
                    worker.slots,
                    theme::age(worker.heartbeat_age_ms)
                ),
            },
            Screen::Run | Screen::Graph => {
                let Some(detail) = &self.detail else {
                    return String::new();
                };
                if let Some(problem) = &detail.view.problem {
                    return format!(
                        "lwwo stopped this run, so that it stopped nothing else: {problem}"
                    );
                }
                let Some(task) = detail.selected_task() else {
                    return "This run has no tasks yet: it is waiting to start.".into();
                };
                let needs: Vec<&str> = task
                    .needs
                    .iter()
                    .map(|&need| detail.view.tasks[need].name.as_str())
                    .collect();
                if let Some(why) = &task.waiting {
                    return format!("{} is ready, but waits for a worker: {why}.", task.name);
                }
                let place = match &task.image {
                    Some(image) => format!(" in {image}"),
                    None => String::new(),
                };
                if needs.is_empty() {
                    format!("{} starts the run{place}. $ {}", task.name, task.command)
                } else {
                    format!(
                        "{} needs {}{place}. $ {}",
                        task.name,
                        needs.join(", "),
                        task.command
                    )
                }
            }
            Screen::Log => match &self.log {
                None => String::new(),
                Some(log) => match &log.task {
                    Some(task) => format!(
                        "{task}, attempt {} of {}, {} lines. TAB chooses stdout or stderr.",
                        log.attempts.get(log.attempt).copied().unwrap_or(0),
                        log.attempts.len(),
                        log.lines.len(),
                    ),
                    None => format!(
                        "The output of {} together, in order of time: {} lines.",
                        plural(log.names.len(), "task"),
                        log.lines.len()
                    ),
                },
            },
        }
    }

    // ---- loading -----------------------------------------------------------

    /// Reads again what the current screen shows. If that fails the old
    /// data stays, and the reason is displayed until a refresh succeeds:
    /// a lasting problem is a state, not a piece of news to repeat.
    fn refresh(&mut self) {
        self.refreshed_at = self.now;
        let result = self
            .load_status()
            .and_then(|()| self.load_runs())
            .and_then(|()| match self.screen {
                Screen::Runs => Ok(()),
                Screen::Dags => self.load_dags(),
                Screen::Workers => self.load_workers(),
                Screen::Run | Screen::Graph | Screen::Log => self.load_detail(),
            });
        self.problem = result.err();
    }

    fn load_status(&mut self) -> Result<()> {
        self.status = self.backend.status()?;
        Ok(())
    }

    fn load_runs(&mut self) -> Result<()> {
        let runs = self.backend.runs(self.only_dag.as_deref(), RUN_LIMIT)?;
        // What changed since the last look is news for the message box.
        if self.loaded {
            for run in &runs {
                let before = self.runs.iter().find(|old| old.id == run.id);
                match before {
                    None if run.state == RunState::Running => {
                        self.dialog
                            .push(format!("A run of {} started ({}).", run.dag, run.trigger));
                    }
                    Some(old) if old.state != run.state && run.state.is_final() => {
                        self.dialog
                            .push(format!("{} {}!", run.dag, run.state.word()));
                    }
                    _ => {}
                }
            }
        }
        self.loaded = true;
        // New runs appear at the top: stay on the same run, not on the
        // same row.
        let selected = self.selected_run().map(|run| run.id.clone());
        self.runs = runs;
        let shown = self.shown_runs();
        let position = selected.and_then(|id| shown.iter().position(|&i| self.runs[i].id == id));
        self.run_cursor = position
            .unwrap_or(self.run_cursor)
            .min(shown.len().saturating_sub(1));
        Ok(())
    }

    fn load_dags(&mut self) -> Result<()> {
        self.dags = self.backend.dags()?;
        self.dag_cursor = self
            .dag_cursor
            .min(self.shown_dags().len().saturating_sub(1));
        Ok(())
    }

    fn load_workers(&mut self) -> Result<()> {
        self.workers = self.backend.workers()?;
        self.worker_cursor = self
            .worker_cursor
            .min(self.shown_workers().len().saturating_sub(1));
        Ok(())
    }

    fn load_detail(&mut self) -> Result<()> {
        let Some(detail) = self.detail.as_mut().filter(|detail| !detail.preview) else {
            return Ok(());
        };
        // The tasks of a run never change, so the cursor stays put.
        detail.view = self.backend.run(&detail.view.run.id)?;
        detail.index();
        Ok(())
    }

    /// Asks for the lines written since the last look.
    fn read_log(&mut self) -> Result<()> {
        self.log_read_at = self.now;
        let (Some(detail), Some(log)) = (&self.detail, &mut self.log) else {
            return Ok(());
        };
        let run = detail.view.run.id.clone();
        let mut grew = false;
        for (index, source) in log.sources.iter_mut().enumerate() {
            let lines = self
                .backend
                .log(&run, &source.task, source.attempt, source.read)?;
            source.read += lines.len();
            grew |= !lines.is_empty();
            log.lines
                .extend(lines.into_iter().map(|line| (index, line)));
        }
        if grew && log.sources.len() > 1 {
            // Several tasks together: in order of time. The sort is
            // stable, so lines of the same instant keep their order.
            log.lines.sort_by_key(|(_, line)| line.at);
        }
        Ok(())
    }

    /// For loads triggered by a key: the next refresh will retry anyway, so
    /// a failure only needs to be shown.
    fn show(&mut self, result: Result<()>) {
        if let Err(error) = result {
            self.problem = Some(error);
        }
    }

    /// Goes to one of the three lists.
    fn go(&mut self, screen: Screen) {
        self.screen = screen;
        self.filter.clear();
        self.detail = None;
        self.log = None;
        let loaded = match screen {
            Screen::Dags => self.load_dags(),
            Screen::Workers => self.load_workers(),
            _ => self.load_runs(),
        };
        self.show(loaded);
    }

    // ---- keys --------------------------------------------------------------

    pub(crate) fn on_key(&mut self, key: KeyEvent) {
        // In raw mode Ctrl-C is a key like any other, not a signal.
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.quit = true;
            return;
        }
        if self.help {
            self.help = false;
            return;
        }
        if self.prompt.is_some() {
            return self.on_prompt_key(key.code);
        }
        if self.confirm.is_some() {
            return self.on_confirm_key(key.code);
        }
        match (self.screen, key.code) {
            (_, KeyCode::Char('q')) => self.quit = true,
            (_, KeyCode::Char('?')) => self.help = true,
            (_, KeyCode::Char(' ')) => self.dialog.skip(self.now),
            (_, KeyCode::Char(':')) => self.prompt = Some((Prompt::Command, String::new())),
            (Screen::Runs | Screen::Dags | Screen::Workers | Screen::Log, KeyCode::Char('/')) => {
                self.prompt = Some((Prompt::Filter, self.filter.clone()));
            }
            // ESC first drops the filter, then leaves the screen.
            (_, KeyCode::Esc) if !self.filter.is_empty() => {
                self.filter.clear();
                self.clamp_cursors();
            }
            (Screen::Runs, code) => self.on_runs_key(code),
            (Screen::Dags, code) => self.on_dags_key(code),
            (Screen::Workers, code) => self.on_workers_key(code),
            (Screen::Run, code) => self.on_run_key(code),
            (Screen::Graph, code) => self.on_graph_key(code),
            (Screen::Log, code) => self.on_log_key(code),
        }
    }

    fn clamp_cursors(&mut self) {
        self.run_cursor = self
            .run_cursor
            .min(self.shown_runs().len().saturating_sub(1));
        self.dag_cursor = self
            .dag_cursor
            .min(self.shown_dags().len().saturating_sub(1));
        self.worker_cursor = self
            .worker_cursor
            .min(self.shown_workers().len().saturating_sub(1));
    }

    fn on_prompt_key(&mut self, code: KeyCode) {
        let Some((kind, mut typed)) = self.prompt.take() else {
            return;
        };
        match code {
            KeyCode::Esc => {
                if kind == Prompt::Filter {
                    self.filter.clear();
                }
            }
            KeyCode::Enter => match kind {
                Prompt::Filter => self.filter = typed,
                Prompt::Command => self.command(typed.trim()),
                Prompt::Params(dag) => self.start_with(dag, &typed),
            },
            KeyCode::Backspace => {
                typed.pop();
                self.prompt = Some((kind, typed));
            }
            KeyCode::Char(c) => {
                typed.push(c);
                self.prompt = Some((kind, typed));
            }
            _ => self.prompt = Some((kind, typed)),
        }
        // A filter applies while it is typed.
        if let Some((Prompt::Filter, typed)) = &self.prompt {
            self.filter.clone_from(typed);
        }
        self.clamp_cursors();
    }

    /// A `:` command: the name of a view, as in k9s.
    fn command(&mut self, command: &str) {
        match command {
            "runs" | "run" | "r" => {
                self.only_dag = None;
                self.go(Screen::Runs);
            }
            "dags" | "dag" | "d" => self.go(Screen::Dags),
            "workers" | "worker" | "w" => self.go(Screen::Workers),
            "q" | "quit" => self.quit = true,
            "" => {}
            other => self.dialog.push(format!(
                "There is no view called {other}. Try runs, dags or workers."
            )),
        }
    }

    fn on_runs_key(&mut self, code: KeyCode) {
        let count = self.shown_runs().len();
        let selected = self
            .selected_run()
            .map(|run| (run.id.clone(), run.dag.clone(), run.state));
        match (code, selected) {
            (KeyCode::Enter | KeyCode::Right | KeyCode::Char('l'), Some((id, ..))) => {
                self.open_run(&id);
            }
            (KeyCode::Char('c'), Some((id, dag, state))) => self.ask_cancel(id, &dag, state),
            (KeyCode::Char('r'), Some((id, dag, state))) => {
                self.ask_rerun(
                    id,
                    state,
                    Rerun::Failed,
                    format!("Run the failed tasks of {dag} again?"),
                );
            }
            (KeyCode::Char('R'), Some((id, dag, state))) => {
                self.ask_rerun(
                    id,
                    state,
                    Rerun::All,
                    format!("Run every task of {dag} again?"),
                );
            }
            (KeyCode::Char('t'), Some((_, dag, _))) => self.ask_start(dag, Vec::new()),
            (KeyCode::Char('T'), Some((_, dag, _))) => {
                self.prompt = Some((Prompt::Params(dag), String::new()));
            }
            (KeyCode::Tab, _) => self.go(Screen::Dags),
            (KeyCode::BackTab, _) => self.go(Screen::Workers),
            (KeyCode::Esc, _) if self.only_dag.is_some() => {
                self.only_dag = None;
                self.go(Screen::Dags);
            }
            (code, _) => move_cursor(&mut self.run_cursor, count, self.page_rows.get(), code),
        }
    }

    fn on_dags_key(&mut self, code: KeyCode) {
        let count = self.shown_dags().len();
        let selected = self
            .selected_dag()
            .map(|dag| (dag.name.clone(), dag.paused));
        match (code, selected) {
            (KeyCode::Enter | KeyCode::Right | KeyCode::Char('l'), Some((name, _))) => {
                self.only_dag = Some(name);
                self.run_cursor = 0;
                self.go(Screen::Runs);
            }
            (KeyCode::Char('g'), Some((name, _))) => self.open_dag(&name),
            (KeyCode::Char('t'), Some((name, _))) => self.ask_start(name, Vec::new()),
            (KeyCode::Char('T'), Some((name, _))) => {
                self.prompt = Some((Prompt::Params(name), String::new()));
            }
            (KeyCode::Char('p'), Some((name, paused))) => {
                let question = if paused {
                    format!("Turn the schedule of {name} on again?")
                } else {
                    format!("Pause {name}? Its schedule stops; runs by hand still work.")
                };
                self.confirm = Some(Confirm {
                    question,
                    yes: true,
                    action: Action::Pause(name, !paused),
                });
            }
            (KeyCode::Tab, _) => self.go(Screen::Workers),
            (KeyCode::BackTab | KeyCode::Esc, _) => {
                self.only_dag = None;
                self.go(Screen::Runs);
            }
            // `g` opens the graph here, so only the arrows go to the top.
            (KeyCode::Char('g'), None) => {}
            (code, _) => move_cursor(&mut self.dag_cursor, count, self.page_rows.get(), code),
        }
    }

    fn on_workers_key(&mut self, code: KeyCode) {
        let count = self.shown_workers().len();
        let selected = self.selected_worker().map(|worker| worker.name.clone());
        match (code, selected) {
            (KeyCode::Char('d'), Some(name)) => {
                self.confirm = Some(Confirm {
                    question: format!(
                        "Drain {name}? It finishes what it has and takes nothing more."
                    ),
                    yes: false,
                    action: Action::Drain(name),
                });
            }
            (KeyCode::Tab | KeyCode::Esc, _) => {
                self.only_dag = None;
                self.go(Screen::Runs);
            }
            (KeyCode::BackTab, _) => self.go(Screen::Dags),
            (code, _) => move_cursor(&mut self.worker_cursor, count, self.page_rows.get(), code),
        }
    }

    /// The keys that the `Run` and `Graph` screens share. Returns false if
    /// the key is not one of them.
    fn on_detail_key(&mut self, code: KeyCode) -> bool {
        let Some(detail) = &self.detail else {
            return false;
        };
        let (id, dag, state) = (
            detail.view.run.id.clone(),
            detail.view.run.dag.clone(),
            detail.view.run.state,
        );
        let task = detail.selected_task().map(|task| task.name.clone());
        match (code, task) {
            (KeyCode::Enter, _) => self.open_log(false),
            (KeyCode::Char('L'), _) => self.open_log(true),
            (KeyCode::Char('c'), _) => self.ask_cancel(id, &dag, state),
            (KeyCode::Char('r'), Some(task)) => {
                let question = format!("Run {task} again, and what depends on it?");
                self.ask_rerun(id, state, Rerun::Task(task), question);
            }
            (KeyCode::Char('f'), _) => {
                self.ask_rerun(
                    id,
                    state,
                    Rerun::Failed,
                    format!("Run the failed tasks of {dag} again?"),
                );
            }
            (KeyCode::Char('R'), _) => {
                self.ask_rerun(
                    id,
                    state,
                    Rerun::All,
                    format!("Run every task of {dag} again?"),
                );
            }
            (KeyCode::Char('t'), _) => self.ask_start(dag, Vec::new()),
            (KeyCode::Char('T'), _) => self.prompt = Some((Prompt::Params(dag), String::new())),
            _ => return false,
        }
        true
    }

    fn on_run_key(&mut self, code: KeyCode) {
        if self.on_detail_key(code) {
            return;
        }
        let Some(detail) = &mut self.detail else {
            self.screen = Screen::Runs;
            return;
        };
        let count = detail.order.len();
        match code {
            KeyCode::Esc | KeyCode::Backspace => {
                self.screen = Screen::Runs;
                self.detail = None;
                self.tasks_offset.set(0);
            }
            KeyCode::Tab => {
                detail.came_from = None;
                self.screen = Screen::Graph;
            }
            KeyCode::Left | KeyCode::Char('h') => detail.move_level(false),
            KeyCode::Right | KeyCode::Char('l') => detail.move_level(true),
            code => move_cursor(&mut detail.cursor, count, self.page_rows.get(), code),
        }
    }

    fn on_graph_key(&mut self, code: KeyCode) {
        let Some(detail) = &mut self.detail else {
            self.screen = Screen::Runs;
            return;
        };
        if detail.preview {
            let name = detail.view.run.dag.clone();
            match code {
                KeyCode::Esc | KeyCode::Backspace => self.go(Screen::Dags),
                KeyCode::Char('t') => self.ask_start(name, Vec::new()),
                KeyCode::Char('T') => self.prompt = Some((Prompt::Params(name), String::new())),
                KeyCode::Enter => self.dialog.push(format!(
                    "This is {name} as a new run would start it: there is no output yet. t starts a run."
                )),
                code => detail.walk(code, self.page_rows.get()),
            }
            return;
        }
        if self.on_detail_key(code) {
            return;
        }
        let Some(detail) = &mut self.detail else {
            return;
        };
        match code {
            KeyCode::Esc | KeyCode::Backspace | KeyCode::Tab => self.screen = Screen::Run,
            code => detail.walk(code, self.page_rows.get()),
        }
    }

    fn on_log_key(&mut self, code: KeyCode) {
        let Some(log) = &mut self.log else {
            self.screen = self.log_from;
            return;
        };
        // Never scroll past the first line: keep one screenful in view.
        let lines = log.visible(&self.filter).count();
        let page = self.page_rows.get().max(1);
        let top = lines.saturating_sub(page);
        match code {
            KeyCode::Esc | KeyCode::Backspace => {
                self.screen = self.log_from;
                self.log = None;
                self.filter.clear();
            }
            KeyCode::Tab | KeyCode::Char('s') => {
                log.show = log.show.next();
                log.scroll_up = 0;
            }
            KeyCode::Char('e') => {
                log.show = if log.show == Show::Err {
                    Show::All
                } else {
                    Show::Err
                };
                log.scroll_up = 0;
            }
            KeyCode::Char('[') if log.attempt > 0 => self.switch_attempt(false),
            KeyCode::Char(']') if log.attempt + 1 < log.attempts.len() => self.switch_attempt(true),
            KeyCode::Up | KeyCode::Char('k') => log.scroll_up = (log.scroll_up + 1).min(top),
            KeyCode::Down | KeyCode::Char('j') => log.scroll_up = log.scroll_up.saturating_sub(1),
            KeyCode::PageUp => log.scroll_up = (log.scroll_up + page).min(top),
            KeyCode::PageDown => log.scroll_up = log.scroll_up.saturating_sub(page),
            KeyCode::Home | KeyCode::Char('g') => log.scroll_up = top,
            KeyCode::End | KeyCode::Char('G' | 'f') => log.scroll_up = 0,
            KeyCode::Left | KeyCode::Char('h') => {
                log.scroll_right = log.scroll_right.saturating_sub(SIDE_STEP);
            }
            KeyCode::Right | KeyCode::Char('l') => log.scroll_right += SIDE_STEP,
            _ => {}
        }
    }

    fn on_confirm_key(&mut self, code: KeyCode) {
        let Some(confirm) = &mut self.confirm else {
            return;
        };
        match code {
            KeyCode::Up
            | KeyCode::Down
            | KeyCode::Left
            | KeyCode::Right
            | KeyCode::Tab
            | KeyCode::Char('j' | 'k') => confirm.yes = !confirm.yes,
            KeyCode::Char('y') => self.answer(true),
            KeyCode::Char('n' | 'q') | KeyCode::Esc => self.answer(false),
            KeyCode::Enter => {
                let yes = confirm.yes;
                self.answer(yes);
            }
            _ => {}
        }
    }

    // ---- actions -----------------------------------------------------------

    fn open_run(&mut self, id: &str) {
        match self.backend.run(id) {
            Ok(view) => {
                // Going back to the list should land on this run.
                let shown = self.shown_runs();
                if let Some(row) = shown.iter().position(|&i| self.runs[i].id == id) {
                    self.run_cursor = row;
                }
                self.detail = Some(Detail::new(view, false));
                self.graph_offset.set((0, 0));
                self.tasks_offset.set(0);
                self.filter.clear();
                self.screen = Screen::Run;
            }
            Err(error) => self.dialog.push(error),
        }
    }

    /// Shows the graph of a DAG, without running it.
    fn open_dag(&mut self, name: &str) {
        match self.backend.dag(name) {
            Ok(view) => {
                self.detail = Some(Detail::new(view, true));
                self.graph_offset.set((0, 0));
                self.filter.clear();
                self.screen = Screen::Graph;
            }
            Err(error) => self.dialog.push(error),
        }
    }

    fn ask_start(&mut self, dag: String, params: Vec<(String, String)>) {
        let with = if params.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
            format!(" with {}", pairs.join(", "))
        };
        self.confirm = Some(Confirm {
            question: format!("Start a run of {dag}{with}?"),
            yes: true,
            action: Action::Start(dag, params),
        });
    }

    /// Reads `name=value name=value` typed after `T` and asks to start.
    fn start_with(&mut self, dag: String, typed: &str) {
        let mut params = Vec::new();
        for pair in typed.split_whitespace() {
            let Some((name, value)) = pair.split_once('=') else {
                self.dialog
                    .push(format!("{pair} is not a parameter: write name=value."));
                return;
            };
            params.push((name.to_owned(), value.to_owned()));
        }
        self.ask_start(dag, params);
    }

    /// Opens the output of the task under the cursor, or of all the tasks.
    fn open_log(&mut self, all: bool) {
        let Some(detail) = &self.detail else {
            return;
        };
        let tasks: Vec<&TaskRow> = if all {
            detail
                .order
                .iter()
                .map(|&task| &detail.view.tasks[task])
                .collect()
        } else {
            detail.selected_task().into_iter().collect()
        };
        let sources: Vec<Source> = tasks
            .iter()
            .filter_map(|task| {
                Some(Source {
                    task: task.name.clone(),
                    attempt: task.attempts.last()?.number,
                    read: 0,
                })
            })
            .collect();
        if sources.is_empty() {
            let what = match (all, tasks.first()) {
                (false, Some(task)) => format!("{} has not started yet", task.name),
                _ => "Nothing has started yet".to_owned(),
            };
            self.dialog.push(format!("{what}: there is no output."));
            return;
        }
        let one = (!all).then(|| tasks[0]);
        self.log = Some(LogView {
            task: one.map(|task| task.name.clone()),
            attempts: one.map_or_else(Vec::new, |task| {
                task.attempts.iter().map(|a| a.number).collect()
            }),
            attempt: one.map_or(0, |task| task.attempts.len() - 1),
            names: sources.iter().map(|source| source.task.clone()).collect(),
            sources,
            lines: Vec::new(),
            // A failed command usually explains itself on stderr, but the
            // lines around it matter too: show everything.
            show: Show::All,
            scroll_up: 0,
            scroll_right: 0,
        });
        self.filter.clear();
        self.log_from = self.screen;
        self.screen = Screen::Log;
        let read = self.read_log();
        self.show(read);
    }

    /// Shows the previous or the next attempt of the task of the log.
    fn switch_attempt(&mut self, forward: bool) {
        let Some(log) = &mut self.log else {
            return;
        };
        log.attempt = if forward {
            log.attempt + 1
        } else {
            log.attempt - 1
        };
        if let (Some(source), Some(&number)) =
            (log.sources.first_mut(), log.attempts.get(log.attempt))
        {
            source.attempt = number;
            source.read = 0;
        }
        log.lines.clear();
        log.scroll_up = 0;
        log.scroll_right = 0;
        let read = self.read_log();
        self.show(read);
    }

    fn ask_cancel(&mut self, run: String, dag: &str, state: RunState) {
        if state.is_final() {
            self.dialog
                .push(format!("This run of {dag} has already finished."));
            return;
        }
        self.confirm = Some(Confirm {
            question: format!("Stop this run of {dag}?"),
            yes: false,
            action: Action::Cancel(run),
        });
    }

    fn ask_rerun(&mut self, run: String, state: RunState, what: Rerun, question: String) {
        if !state.is_final() {
            self.dialog
                .push("Only a finished run can be run again: stop it first, or wait.");
            return;
        }
        self.confirm = Some(Confirm {
            question,
            yes: false,
            action: Action::Rerun(run, what),
        });
    }

    fn answer(&mut self, yes: bool) {
        let Some(confirm) = self.confirm.take() else {
            return;
        };
        if !yes {
            return;
        }
        let done = match confirm.action {
            Action::Start(dag, params) => self.backend.start(&dag, &params).map(|run| {
                // Started from the graph of the DAG: the graph of the run,
                // on the same task, comes to life in its place.
                let looking = self
                    .detail
                    .as_ref()
                    .filter(|detail| detail.preview && self.screen == Screen::Graph)
                    .and_then(Detail::selected_task)
                    .map(|task| task.name.clone());
                if let Some(name) = looking {
                    self.open_run(&run);
                    if let Some(detail) = &mut self.detail {
                        if let Some(task) = detail.view.tasks.iter().position(|t| t.name == name) {
                            detail.select(task);
                        }
                        self.screen = Screen::Graph;
                    }
                }
                format!("Go, {dag}!")
            }),
            Action::Cancel(run) => self
                .backend
                .cancel(&run)
                .map(|()| "The run was told to stop.".to_owned()),
            Action::Rerun(run, what) => self.backend.rerun(&run, &what).map(|()| match what {
                Rerun::Task(task) => format!("{task}, once more!"),
                Rerun::Failed | Rerun::All => "Once more!".to_owned(),
            }),
            Action::Pause(dag, paused) => self.backend.pause(&dag, paused).map(|()| {
                if paused {
                    format!("{dag} is paused.")
                } else {
                    format!("{dag} is back on schedule.")
                }
            }),
            Action::Drain(worker) => self
                .backend
                .drain(&worker)
                .map(|()| format!("{worker} is draining.")),
        };
        match done {
            Ok(message) => self.dialog.push(message),
            Err(error) => self.dialog.push(error),
        }
        self.refresh();
    }
}

/// "1 task", "3 tasks".
pub(crate) fn plural(count: usize, noun: &str) -> String {
    match count {
        1 => format!("1 {noun}"),
        _ => format!("{count} {noun}s"),
    }
}

/// Moves a list cursor in answer to an arrow, page or home/end key.
fn move_cursor(cursor: &mut usize, count: usize, page: usize, code: KeyCode) {
    let last = count.saturating_sub(1);
    let page = page.max(1);
    *cursor = match code {
        KeyCode::Up | KeyCode::Char('k') => cursor.saturating_sub(1),
        KeyCode::Down | KeyCode::Char('j') => (*cursor + 1).min(last),
        KeyCode::PageUp => cursor.saturating_sub(page),
        KeyCode::PageDown => (*cursor + page).min(last),
        KeyCode::Home | KeyCode::Char('g') => 0,
        KeyCode::End | KeyCode::Char('G') => last,
        _ => *cursor,
    };
}
