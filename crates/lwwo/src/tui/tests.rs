//! The interface driven without a terminal: keys in, screen out.

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{App, Screen};
use super::demo::Demo;
use super::ui::snapshot;

/// A fixed instant, so that every screen is the same at every run.
const NOW: i64 = 1_793_066_400_000;

struct Tui {
    app: App<Demo>,
    now: i64,
}

impl Tui {
    fn new() -> Self {
        let mut tui = Self {
            app: App::new(Demo::new(NOW), NOW),
            now: NOW,
        };
        // Load the data, then let the welcome message be typed and go.
        tui.wait(10_000);
        tui.wait(10_000);
        tui
    }

    fn wait(&mut self, millis: i64) {
        self.now += millis;
        self.app.on_tick(self.now);
    }

    fn press(&mut self, code: KeyCode) {
        self.app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
        self.app.on_tick(self.now);
    }

    fn keys(&mut self, text: &str) {
        for c in text.chars() {
            self.press(KeyCode::Char(c));
        }
    }

    fn screen(&self) -> String {
        snapshot(&self.app, 110, 30)
    }

    /// The screen a moment later, with the message of the moment typed out.
    fn settled(&mut self) -> String {
        self.wait(50);
        self.wait(1_000);
        self.screen()
    }

    /// Waits until all of `wanted` is on screen. Messages take turns in
    /// the message box, so the one looked for may come after another.
    #[track_caller]
    fn shows(&mut self, wanted: &[&str]) {
        for _ in 0..60 {
            self.wait(250);
            let screen = self.screen();
            if wanted.iter().all(|text| screen.contains(text)) {
                return;
            }
        }
        let screen = self.screen();
        let missing: Vec<&&str> = wanted
            .iter()
            .filter(|text| !screen.contains(**text))
            .collect();
        panic!("{missing:?} never came on screen:\n{screen}");
    }

    #[track_caller]
    fn hides(&mut self, unwanted: &str) {
        let screen = self.settled();
        assert!(
            !screen.contains(unwanted),
            "`{unwanted}` is on screen:\n{screen}"
        );
    }
}

#[test]
fn the_first_screen_lists_the_runs_with_their_health() {
    let mut tui = Tui::new();
    tui.shows(&[
        "LWWO",
        "RUNS",
        "workers 2  slots 1/12",
        "▶ train_model",
        "QUEUED",
        "RUNNING",
        "FAILED",
        "SUCCESS",
        "8/8",
        "██████████",
        "cron",
        // The message box describes what is under the cursor.
        "train_model is queued: 0 of 2 tasks done",
        "<⏎> open",
    ]);
}

#[test]
fn a_run_opens_on_its_levels_and_tasks() {
    let mut tui = Tui::new();
    tui.press(KeyCode::Down);
    tui.press(KeyCode::Enter);
    assert_eq!(tui.app.screen, Screen::Run);
    tui.shows(&[
        "╔ RUN ═",
        "etl_sales",
        ":L2/5",
        "HP:",
        "3/8",
        "╔ LEVELS ═",
        "L1",
        "L5",
        "╔ TASKS ═",
        // The cursor starts where the action is: the running task.
        "▶ ● enrich",
        "enrich needs extract. $ python enrich.py",
        "<tab> graph",
    ]);
    // Left and right move across the levels.
    tui.press(KeyCode::Left);
    tui.shows(&["extract starts the run"]);
    tui.press(KeyCode::Esc);
    assert_eq!(tui.app.screen, Screen::Runs);
}

#[test]
fn the_graph_draws_every_dependency_and_walks_along_them() {
    let mut tui = Tui::new();
    tui.press(KeyCode::Down);
    tui.press(KeyCode::Enter);
    tui.press(KeyCode::Tab);
    assert_eq!(tui.app.screen, Screen::Graph);
    let screen = tui.settled();
    for line in [
        "┌─▶ ✓ clean ──┬┬─▶ · model-a ─┐",
        "✓ extract ─┼─▶ ● enrich ─┴┴─▶ · model-b ─┼─▶ · report ──▶ · publish",
        "└─▶ ✓ check ──────────────────┘",
    ] {
        assert!(
            screen.contains(line),
            "`{line}` is not on screen:\n{screen}"
        );
    }
    // Right follows a line to a task that needs this one, left goes back.
    tui.press(KeyCode::Right);
    tui.shows(&["model-b needs clean, enrich"]);
    tui.press(KeyCode::Left);
    tui.shows(&["enrich needs extract"]);
    tui.press(KeyCode::Tab);
    assert_eq!(tui.app.screen, Screen::Run);
}

#[test]
fn the_output_of_a_task_has_attempts_streams_and_search() {
    let mut tui = Tui::new();
    tui.keys("jj");
    tui.press(KeyCode::Enter);
    // The failed run opens on its failed task, which tried twice.
    tui.shows(&["▶ ✗ model-a", "timeout"]);
    tui.press(KeyCode::Enter);
    assert_eq!(tui.app.screen, Screen::Log);
    tui.shows(&[
        "model-a #2",
        "18432 rows read",
        "timeout on the connection",
        "timeout: stopping the task",
        "following",
        "attempt 2 of 2",
    ]);
    tui.keys("[");
    tui.shows(&["model-a #1", "attempt 1 of 2"]);
    tui.hides("stopping the task");
    // Only stderr.
    tui.keys("e");
    tui.shows(&["timeout on the connection"]);
    tui.hides("18432 rows read");
    tui.keys("e/");
    tui.keys("rows");
    tui.press(KeyCode::Enter);
    tui.shows(&["18432 rows read", "dropping 112 rows", "/ rows"]);
    tui.hides("writing s3");
    tui.press(KeyCode::Esc);
    tui.press(KeyCode::Esc);
    assert_eq!(tui.app.screen, Screen::Run);
}

#[test]
fn all_the_tasks_of_a_run_can_talk_together() {
    let mut tui = Tui::new();
    tui.keys("jjj");
    tui.press(KeyCode::Enter);
    tui.keys("L");
    tui.shows(&[
        "ALL TASKS",
        "extract",
        "publish",
        "The output of 8 tasks together",
    ]);
}

#[test]
fn actions_ask_first_and_say_how_it_went() {
    let mut tui = Tui::new();
    // Stopping a run: the answer starts on NO.
    tui.press(KeyCode::Down);
    tui.keys("c");
    tui.shows(&["Stop this run of etl_sales?", "▶NO", "<y> yes"]);
    tui.press(KeyCode::Enter);
    tui.shows(&["RUNNING"]);
    tui.keys("cy");
    tui.shows(&["The run was told to stop.", "CANCELLED"]);

    // A finished run can be run again; a running one cannot.
    tui.keys("r");
    tui.keys("y");
    tui.shows(&["Once more!"]);
    tui.keys("r");
    tui.shows(&["Only a finished run can be run again"]);
}

#[test]
fn dags_start_pause_and_show_their_graph() {
    let mut tui = Tui::new();
    tui.press(KeyCode::Tab);
    assert_eq!(tui.app.screen, Screen::Dags);
    tui.shows(&[
        "╔ DAGS ═",
        "▶ etl_sales",
        "ACTIVE",
        "PAUSED",
        "by hand",
        "runs next at",
    ]);

    tui.keys("p");
    tui.shows(&["Pause etl_sales?"]);
    tui.keys("y");
    tui.shows(&["etl_sales is paused."]);

    // The graph of a DAG, without running it; starting from there lands on
    // the graph of the new run.
    tui.keys("g");
    assert_eq!(tui.app.screen, Screen::Graph);
    tui.shows(&[
        "8 tasks on 5 levels, as a new run would start them",
        "· extract",
    ]);
    tui.keys("t");
    tui.shows(&["Start a run of etl_sales?", "▶YES"]);
    tui.press(KeyCode::Enter);
    tui.shows(&["Go, etl_sales!", "GRAPH"]);

    // With parameters.
    tui.press(KeyCode::Esc);
    tui.press(KeyCode::Esc);
    tui.press(KeyCode::Tab);
    tui.keys("T");
    tui.shows(&["Parameters for etl_sales, as name=value:"]);
    tui.keys("country=FR");
    tui.press(KeyCode::Enter);
    tui.shows(&["Start a run of etl_sales with country=FR?"]);
    tui.keys("n");

    // Enter lists only the runs of the DAG.
    tui.press(KeyCode::Enter);
    assert_eq!(tui.app.screen, Screen::Runs);
    tui.shows(&["RUNS of etl_sales"]);
    tui.hides("train_model");
}

#[test]
fn workers_are_listed_and_drained() {
    let mut tui = Tui::new();
    tui.keys(":workers");
    tui.press(KeyCode::Enter);
    assert_eq!(tui.app.screen, Screen::Workers);
    tui.shows(&[
        "╔ WORKERS ═",
        "▶ w1",
        "READY",
        "1/8",
        "12.0G",
        "big,docker",
        "w1 is running 1 task of the 8",
    ]);
    tui.keys("d");
    tui.shows(&["Drain w1?"]);
    tui.keys("y");
    tui.shows(&["w1 is draining.", "DRAINING"]);
}

#[test]
fn a_filter_narrows_a_list_and_escape_clears_it() {
    let mut tui = Tui::new();
    tui.keys("/fail");
    tui.shows(&["Filter: fail", "FAILED"]);
    tui.hides("QUEUED");
    tui.press(KeyCode::Enter);
    tui.shows(&["/ fail", "▶ etl_sales"]);
    tui.press(KeyCode::Esc);
    tui.shows(&["QUEUED", "RUNNING"]);
    // A view that does not exist is said, not ignored.
    tui.keys(":pods");
    tui.press(KeyCode::Enter);
    tui.shows(&["There is no view called pods"]);
}

#[test]
fn help_lists_the_keys_and_a_small_terminal_is_told_so() {
    let mut tui = Tui::new();
    tui.keys("?");
    tui.shows(&["╔ KEYS ═", "drain a worker", "q quits"]);
    tui.keys("x");
    tui.hides("╔ KEYS ═");
    assert!(snapshot(&tui.app, 40, 10).contains("LWWO needs at least 64x18"));
    tui.keys("q");
    assert!(tui.app.should_quit());
}

#[test]
fn a_task_no_worker_can_take_says_why() {
    let mut tui = Tui::new();
    tui.press(KeyCode::End);
    tui.press(KeyCode::Enter);
    tui.shows(&[
        "○ train",
        "READY",
        "train is ready, but waits for a worker: no connected worker has the label \"gpu\".",
    ]);
}
