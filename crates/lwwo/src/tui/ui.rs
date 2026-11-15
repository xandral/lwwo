//! Drawing: turns the state of an [`App`] into a screen.
//!
//! Every function here is a pure description of what to show for a given
//! state and area. Lists are drawn as plain lines of text with fixed-width
//! columns, which keeps the layout explicit and easy to test.

use std::cell::Cell;
use std::ops::Range;

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::app::{App, Detail, LogView, Prompt, Screen, Show, plural};
use super::backend::{Backend, RunRow, TaskRow};
use super::dialog::wrap;
use super::graph::{self, Window};
use super::theme::{self, CURSOR, MORE, fit, fit_middle, fit_right};

/// Below this size the screens do not fit.
const MIN_WIDTH: u16 = 64;
const MIN_HEIGHT: u16 = 18;
/// Width of the status box of a run, borders included.
const HUD_WIDTH: u16 = 34;
/// Longest task name shown in full in the levels.
const LEVEL_NAME_WIDTH: usize = 20;
/// Cells kept in view on either side of the selected task in the graph.
const GRAPH_MARGIN: usize = 4;

pub(crate) fn draw<B: Backend>(app: &App<B>, frame: &mut Frame) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        let text = format!("LWWO needs at least {MIN_WIDTH}x{MIN_HEIGHT} characters.");
        frame.render_widget(Paragraph::new(text), area);
        return;
    }
    let [header, main, dialog, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(4),
        Constraint::Length(1),
    ])
    .areas(area);

    draw_header(app, frame, header);
    match (app.screen, &app.detail, &app.log) {
        (Screen::Runs, ..) => draw_runs(app, frame, main),
        (Screen::Dags, ..) => draw_dags(app, frame, main),
        (Screen::Workers, ..) => draw_workers(app, frame, main),
        (Screen::Run, Some(detail), _) => draw_run(app, detail, frame, main),
        (Screen::Graph, Some(detail), _) => draw_graph_screen(app, detail, frame, main),
        (Screen::Log, _, Some(log)) => draw_log(app, log, frame, main),
        _ => {}
    }
    draw_dialog(app, frame, dialog);
    draw_footer(app, frame, footer);
    if let Some(confirm) = &app.confirm {
        draw_yes_no(confirm.yes, frame, dialog);
    }
    if app.help {
        draw_help(frame, area);
    }
}

/// The screen as text, one line per row: what the tests compare.
#[cfg(test)]
pub(crate) fn snapshot<B: Backend>(app: &App<B>, width: u16, height: u16) -> String {
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, height))
        .expect("a test terminal");
    terminal
        .draw(|frame| draw(app, frame))
        .expect("drawing on a test terminal");
    let buffer = terminal.backend().buffer();
    (0..height)
        .map(|y| {
            let line: String = (0..width).map(|x| buffer[(x, y)].symbol()).collect();
            line.trim_end().to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---- header and footer -----------------------------------------------------

fn draw_header<B: Backend>(app: &App<B>, frame: &mut Frame, area: Rect) {
    let tab = |label: &'static str, active: bool| {
        let style = if active {
            theme::selected()
        } else {
            theme::dim()
        };
        Span::styled(format!(" {label} "), style)
    };
    let in_runs = !matches!(app.screen, Screen::Dags | Screen::Workers);
    let preview = app.detail.as_ref().is_some_and(|detail| detail.preview);
    let mut left = vec![
        Span::styled(" LWWO ", theme::bold()),
        tab("RUNS", in_runs && !preview),
        tab("DAGS", app.screen == Screen::Dags || preview),
        tab("WORKERS", app.screen == Screen::Workers),
    ];
    // Where we are inside RUNS: the run and which of its two screens, or
    // the task whose log is open.
    if let (Screen::Run | Screen::Graph | Screen::Log, Some(detail)) = (app.screen, &app.detail) {
        left.push(Span::raw(format!(" {CURSOR} {}", detail.view.run.dag)));
        if let (Screen::Log, Some(log)) = (app.screen, &app.log) {
            let what = log.task.as_deref().unwrap_or("all tasks");
            left.push(Span::raw(format!(" {CURSOR} {what}")));
        } else if !detail.preview {
            left.push(Span::raw(" "));
            left.push(tab("TASKS", app.screen == Screen::Run));
            left.push(tab("GRAPH", app.screen == Screen::Graph));
        }
    }
    frame.render_widget(Paragraph::new(Line::from(left)), area);

    let status = &app.status;
    let totals = Line::from(vec![
        Span::styled(format!("{} ", status.address), theme::dim()),
        Span::raw(format!(
            "workers {}  slots {}/{} ",
            status.workers, status.slots_used, status.slots
        )),
        Span::styled(
            format!(" ● {} ", status.active_runs),
            Style::new().fg(Color::Yellow),
        ),
        Span::styled(
            format!(" … {} ", status.queued_runs),
            Style::new().fg(Color::Cyan),
        ),
    ]);
    frame.render_widget(Paragraph::new(totals).alignment(Alignment::Right), area);
}

fn draw_footer<B: Backend>(app: &App<B>, frame: &mut Frame, area: Rect) {
    let keys: &[(&str, &str)] = if app.prompt.is_some() {
        &[("⏎", "done"), ("esc", "never mind")]
    } else if app.confirm.is_some() {
        &[
            ("↑↓", "choose"),
            ("⏎", "confirm"),
            ("y", "yes"),
            ("n", "no"),
        ]
    } else {
        match (app.screen, app.detail.as_ref().is_some_and(|d| d.preview)) {
            (Screen::Runs, _) => &[
                ("⏎", "open"),
                ("t", "start"),
                ("c", "stop"),
                ("r", "rerun failed"),
                ("/", "filter"),
                ("tab", "dags"),
                ("?", "help"),
            ],
            (Screen::Dags, _) => &[
                ("⏎", "runs"),
                ("g", "graph"),
                ("t", "start"),
                ("T", "with params"),
                ("p", "pause"),
                ("tab", "workers"),
                ("?", "help"),
            ],
            (Screen::Workers, _) => &[
                ("d", "drain"),
                ("/", "filter"),
                ("tab", "runs"),
                ("?", "help"),
            ],
            (Screen::Run, _) => &[
                ("⏎", "output"),
                ("L", "all output"),
                ("r", "rerun task"),
                ("c", "stop"),
                ("tab", "graph"),
                ("esc", "back"),
                ("?", "help"),
            ],
            (Screen::Graph, true) => &[("↑↓←→", "move"), ("t", "start a run"), ("esc", "back")],
            (Screen::Graph, false) => &[
                ("↑↓←→", "move"),
                ("⏎", "output"),
                ("r", "rerun task"),
                ("tab", "tasks"),
                ("esc", "back"),
            ],
            (Screen::Log, _) => &[
                ("↑↓←→", "scroll"),
                ("tab", "stream"),
                ("[ ]", "attempt"),
                ("/", "search"),
                ("f", "follow"),
                ("esc", "back"),
            ],
        }
    };
    let mut spans = Vec::new();
    for (key, label) in keys {
        spans.push(Span::styled(
            format!(" <{key}>"),
            Style::new().fg(Color::Cyan),
        ));
        spans.push(Span::styled(format!(" {label} "), theme::dim()));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

// ---- the message box -------------------------------------------------------

fn draw_dialog<B: Backend>(app: &App<B>, frame: &mut Frame, area: Rect) {
    let block = theme::window("");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let width = usize::from(inner.width.saturating_sub(2));
    let rows = usize::from(inner.height);

    // What is being typed; else a question waiting for its answer; else a
    // lasting problem; else the news; else a description of what is under
    // the cursor.
    let (lines, style) = if let Some((kind, typed)) = &app.prompt {
        let label = match kind {
            Prompt::Filter if app.screen == Screen::Log => "Search: ".to_owned(),
            Prompt::Filter => "Filter: ".to_owned(),
            Prompt::Command => "Go to (runs, dags, workers): ".to_owned(),
            Prompt::Params(dag) => format!("Parameters for {dag}, as name=value: "),
        };
        (
            wrap(&format!("{label}{typed}▌"), width, rows),
            theme::bold(),
        )
    } else if let Some(confirm) = &app.confirm {
        (wrap(&confirm.question, width, rows), Style::new())
    } else if let Some(problem) = &app.problem {
        (wrap(problem, width, rows), Style::new().fg(Color::Red))
    } else if let Some(lines) = app.dialog.lines(width, rows) {
        (lines, Style::new())
    } else {
        (wrap(&app.hint(), width, rows), theme::dim())
    };
    let text: Vec<Line> = lines
        .into_iter()
        .map(|line| Line::styled(format!(" {line}"), style))
        .collect();
    frame.render_widget(Paragraph::new(text), inner);

    // More to read: the blinking arrow of the old text boxes.
    if app.dialog.waiting() > 0 && app.now / 400 % 2 == 0 && inner.width > 2 {
        let corner = Rect::new(inner.right() - 2, inner.bottom() - 1, 1, 1);
        frame.render_widget(Paragraph::new(MORE), corner);
    }
}

/// The small YES/NO menu that pops up above the message box.
fn draw_yes_no(yes: bool, frame: &mut Frame, dialog: Rect) {
    let area = Rect::new(
        dialog.right().saturating_sub(12),
        dialog.y.saturating_sub(4),
        10,
        4,
    );
    let choice = |label: &str, chosen: bool| {
        let cursor = if chosen { CURSOR } else { " " };
        Line::styled(format!(" {cursor}{label}"), theme::bold())
    };
    let menu =
        Paragraph::new(vec![choice("YES", yes), choice("NO", !yes)]).block(theme::window(""));
    frame.render_widget(Clear, area);
    frame.render_widget(menu, area);
}

fn draw_help(frame: &mut Frame, area: Rect) {
    const LINES: &[&str] = &[
        "",
        " Views    TAB goes round RUNS, DAGS and WORKERS",
        "          :runs  :dags  :workers go there by name",
        " Move     ↑ ↓ or j k    PgUp PgDn    g top    G bottom",
        "          ⏎ opens    ESC goes back    / filters    SPACE skips a message",
        "",
        " t        start a run of the DAG        T asks for parameters first",
        " c        stop the run",
        " r        run again: in the list of runs, the failed tasks;",
        "          inside a run, the selected task and what depends on it",
        " f / R    inside a run: the failed tasks / every task",
        " ⏎ / L    the output of the task / of all the tasks together",
        " p        pause or resume the schedule of a DAG",
        " g        the graph of a DAG, without running it",
        " d        drain a worker: it finishes what it has, takes nothing more",
        "",
        " In the graph, ← and → follow the lines to what a task needs",
        " and to what needs it.",
        "",
        " In an output: TAB all, stdout, stderr    e errors only",
        "               [ ] other attempts    f back to the end",
        "",
        " ✓ success  ● running  · waiting  ↻ retrying  ✗ failed",
        " - skipped  × cancelled",
        "",
        " q quits. Any key closes this.",
    ];
    let width = 74.min(area.width);
    let height = u16::try_from(LINES.len() + 2)
        .unwrap_or(u16::MAX)
        .min(area.height);
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    let text: Vec<Line> = LINES.iter().map(|&line| Line::raw(line)).collect();
    frame.render_widget(Clear, popup);
    frame.render_widget(Paragraph::new(text).block(theme::window("KEYS")), popup);
}

// ---- lists -----------------------------------------------------------------

/// Which rows of a list to draw so that the cursor stays visible. The first
/// visible row is remembered in `offset`, so the list only scrolls when the
/// cursor reaches an edge.
fn visible_rows(cursor: usize, count: usize, rows: usize, offset: &Cell<usize>) -> Range<usize> {
    let mut first = offset.get().min(count.saturating_sub(rows));
    if cursor < first {
        first = cursor;
    } else if rows > 0 && cursor >= first + rows {
        first = cursor + 1 - rows;
    }
    offset.set(first);
    first..(first + rows).min(count)
}

/// What a list is made of, apart from its rows.
struct List<'a> {
    title: String,
    header: String,
    cursor: usize,
    count: usize,
    offset: &'a Cell<usize>,
    /// Shown in place of the rows when there are none.
    empty: &'a str,
}

/// Draws a titled window with a header line and one line per visible row:
/// `row` is called for each visible index.
fn draw_list<B: Backend>(
    app: &App<B>,
    frame: &mut Frame,
    area: Rect,
    list: &List<'_>,
    row: impl Fn(usize, bool) -> Line<'static>,
) {
    let mut block = theme::window(&list.title);
    if !app.filter.is_empty() && app.screen != Screen::Log {
        let filter = format!(" / {} ", app.filter);
        block =
            block.title_top(Line::styled(filter, Style::new().fg(Color::Yellow)).right_aligned());
    }
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let rows = usize::from(inner.height.saturating_sub(1));
    app.page_rows.set(rows);

    let mut lines = vec![Line::styled(list.header.clone(), theme::dim())];
    if list.count == 0 {
        lines.push(Line::styled(format!("  {}", list.empty), theme::dim()));
    }
    for index in visible_rows(list.cursor, list.count, rows, list.offset) {
        lines.push(row(index, index == list.cursor));
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

/// `▶ ` in front of the selected row, two spaces otherwise.
fn cursor_mark(selected: bool) -> Span<'static> {
    Span::styled(if selected { "▶ " } else { "  " }, theme::bold())
}

/// The name column: bold on the selected row.
fn name_cell(text: &str, width: usize, selected: bool) -> Span<'static> {
    let style = if selected {
        theme::bold()
    } else {
        Style::new()
    };
    Span::styled(fit(text, width), style)
}

fn draw_runs<B: Backend>(app: &App<B>, frame: &mut Frame, area: Rect) {
    // cursor, DAG, RUN, STATE, HP bar + count, BY, WHEN, TIME
    let shown = app.shown_runs();
    let fixed = 2 + 2 + 10 + 10 + 18 + 8 + 12 + 8 + 7;
    let dag_width = usize::from(area.width).saturating_sub(fixed).max(8);
    let header = format!(
        "  {} {} {} {} {} {} {}",
        fit("DAG", dag_width),
        fit("RUN", 10),
        fit("STATE", 10),
        fit("HP", 18),
        fit("BY", 8),
        fit("FOR", 12),
        fit_right("TIME", 8),
    );
    let row = |index: usize, selected: bool| {
        let run = &app.runs[shown[index]];
        let (label, style) = theme::run_state(run.state);
        let mut spans = vec![
            cursor_mark(selected),
            name_cell(&run.dag, dag_width, selected),
            // The end of a UUIDv7 is random: it tells runs apart.
            Span::styled(format!(" {} ", fit_middle(&run.id, 10)), theme::dim()),
            Span::styled(fit(label, 10), style),
            Span::raw(" "),
        ];
        spans.extend(theme::hp_bar(run.tasks, 10));
        let done = format!("{}/{}", run.tasks.succeeded, run.tasks.total);
        spans.push(Span::raw(format!(
            " {} {} {} {}",
            fit(&done, 7),
            fit(&run.trigger, 8),
            fit(&theme::when(run.logical_time, app.now), 12),
            fit_right(&run_time(run, app.now), 8),
        )));
        Line::from(spans)
    };
    let title = match &app.only_dag {
        Some(dag) => format!("RUNS of {dag}"),
        None => "RUNS".to_owned(),
    };
    let list = List {
        title,
        header,
        cursor: app.run_cursor,
        count: shown.len(),
        offset: &app.runs_offset,
        empty: "no runs",
    };
    draw_list(app, frame, area, &list, row);
}

/// How long a run has been going, or took.
fn run_time(run: &RunRow, now: i64) -> String {
    match (run.started_at, run.finished_at) {
        (Some(started), Some(finished)) => theme::elapsed(finished - started),
        (Some(started), None) => theme::elapsed(now - started),
        _ => String::new(),
    }
}

fn draw_dags<B: Backend>(app: &App<B>, frame: &mut Frame, area: Rect) {
    // cursor, DAG, STATE, NEXT RUN, IN, VERSION
    let shown = app.shown_dags();
    let name_width = usize::from(area.width).saturating_sub(2 + 2 + 8 + 13 + 6 + 13 + 5);
    let header = format!(
        "  {} {} {} {} {}",
        fit("DAG", name_width),
        fit("STATE", 8),
        fit("NEXT RUN", 13),
        fit_right("IN", 6),
        fit("VERSION", 13),
    );
    let row = |index: usize, selected: bool| {
        let dag = &app.dags[shown[index]];
        let (state, style) = if dag.paused {
            ("PAUSED", Style::new().fg(Color::Magenta))
        } else {
            ("ACTIVE", Style::new().fg(Color::Green))
        };
        let (next, wait) = match dag.next_fire {
            Some(next) => (theme::when(next, app.now), theme::age(next - app.now)),
            None => ("by hand".to_owned(), String::new()),
        };
        Line::from(vec![
            cursor_mark(selected),
            name_cell(&dag.name, name_width, selected),
            Span::raw(" "),
            Span::styled(fit(state, 8), style),
            Span::raw(format!(" {} {} ", fit(&next, 13), fit_right(&wait, 6))),
            Span::styled(fit(&dag.version, 13), theme::dim()),
        ])
    };
    let list = List {
        title: "DAGS".to_owned(),
        header,
        cursor: app.dag_cursor,
        count: shown.len(),
        offset: &app.dags_offset,
        empty: "no DAGs: lwwo apply my-dag.yaml",
    };
    draw_list(app, frame, area, &list, row);
}

fn draw_workers<B: Backend>(app: &App<B>, frame: &mut Frame, area: Rect) {
    // cursor, WORKER, STATE, SLOTS bar + count, MEM, DISK, SEEN, LABELS
    let shown = app.shown_workers();
    let free = usize::from(area.width).saturating_sub(2 + 2 + 9 + 18 + 7 + 7 + 5 + 8);
    let name_width = (free / 2).max(8);
    let labels_width = free.saturating_sub(name_width);
    let header = format!(
        "  {} {} {} {} {} {} {}",
        fit("WORKER", name_width),
        fit("STATE", 9),
        fit("SLOTS", 18),
        fit_right("MEM", 7),
        fit_right("DISK", 7),
        fit_right("SEEN", 5),
        fit("LABELS", labels_width),
    );
    let row = |index: usize, selected: bool| {
        let worker = &app.workers[shown[index]];
        let (state, style) = if worker.accepting {
            ("READY", Style::new().fg(Color::Green))
        } else {
            ("DRAINING", Style::new().fg(Color::Magenta))
        };
        let size = |bytes: u64| match bytes {
            0 => String::new(),
            bytes => theme::bytes(bytes),
        };
        let cells = 10usize;
        let used = (worker.used as usize * cells)
            .checked_div(worker.slots as usize)
            .unwrap_or(0)
            .min(cells);
        let mut tags = worker.labels.clone();
        tags.extend(worker.executors.iter().filter(|e| *e != "process").cloned());
        Line::from(vec![
            cursor_mark(selected),
            name_cell(&worker.name, name_width, selected),
            Span::raw(" "),
            Span::styled(fit(state, 9), style),
            Span::raw(" "),
            Span::styled("█".repeat(used), Style::new().fg(Color::Yellow)),
            Span::styled("░".repeat(cells - used), theme::dim()),
            Span::raw(format!(
                " {} {} {} {} {}",
                fit(&format!("{}/{}", worker.used, worker.slots), 7),
                fit_right(&size(worker.available_memory), 7),
                fit_right(&size(worker.free_disk), 7),
                fit_right(&theme::age(worker.heartbeat_age_ms), 5),
                fit(&tags.join(","), labels_width),
            )),
        ])
    };
    let list = List {
        title: "WORKERS".to_owned(),
        header,
        cursor: app.worker_cursor,
        count: shown.len(),
        offset: &app.workers_offset,
        empty: "no worker is connected",
    };
    draw_list(app, frame, area, &list, row);
}

// ---- one run ---------------------------------------------------------------

fn draw_run<B: Backend>(app: &App<B>, detail: &Detail, frame: &mut Frame, area: Rect) {
    // The levels get the height of the tallest one, up to half the screen:
    // the task list below keeps the other half. The whole graph has a
    // screen of its own.
    let tallest =
        u16::try_from(detail.levels.iter().map(Vec::len).max().unwrap_or(0)).unwrap_or(u16::MAX);
    let top_height = tallest.saturating_add(3).clamp(7, (area.height / 2).max(7));
    let [top, tasks] =
        Layout::vertical([Constraint::Length(top_height), Constraint::Min(0)]).areas(area);
    let [hud, levels] =
        Layout::horizontal([Constraint::Length(HUD_WIDTH), Constraint::Min(0)]).areas(top);

    draw_hud(app, detail, frame, hud);
    draw_levels(detail, frame, levels);
    draw_tasks(app, detail, frame, tasks);
}

/// The level being worked on, the first one with a task not yet settled,
/// out of all: `:L2/5`.
fn level_reached(detail: &Detail) -> String {
    let levels = detail.levels.len();
    let open = |level: &Vec<usize>| {
        level
            .iter()
            .any(|&task| !detail.view.tasks[task].state.is_final())
    };
    let current = detail
        .levels
        .iter()
        .position(open)
        .map_or(levels, |level| level + 1);
    format!(":L{current}/{levels}")
}

/// The status box of a run, in the manner of a battle screen: name, level,
/// HP bar.
fn draw_hud<B: Backend>(app: &App<B>, detail: &Detail, frame: &mut Frame, area: Rect) {
    let block = theme::window("RUN");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let width = usize::from(inner.width.saturating_sub(2));
    let run = &detail.view.run;

    let level = level_reached(detail);
    let (label, style) = theme::run_state(run.state);
    let done = format!("{}/{}", run.tasks.succeeded, run.tasks.total);
    let bar_width = width.saturating_sub(4 + done.len());
    let mut hp = vec![Span::styled(" HP:", theme::bold())];
    hp.extend(theme::hp_bar(run.tasks, bar_width));
    hp.push(Span::raw(format!(" {done}")));

    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                format!(" {}", fit(&run.dag, width.saturating_sub(level.len()))),
                theme::bold(),
            ),
            Span::raw(level),
        ]),
        Line::from(hp),
        Line::from(vec![
            Span::styled(format!(" {label}"), style),
            Span::raw(format!("  {}", run_time(run, app.now))),
        ]),
        Line::styled(
            format!(
                " {} for {}",
                run.trigger,
                theme::when(run.logical_time, app.now)
            ),
            theme::dim(),
        ),
    ];
    if detail.view.params != "{}" {
        lines.push(Line::styled(
            format!(" {}", fit(&detail.view.params, width)),
            theme::dim(),
        ));
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

/// The levels of the run: one column per level, roots on the left. The
/// dependencies are not drawn (the GRAPH screen does that); instead the
/// tasks linked to the one under the cursor are underlined, those it needs
/// on its left and those needing it on its right.
fn draw_levels(detail: &Detail, frame: &mut Frame, area: Rect) {
    let block = theme::window("LEVELS");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let rows = usize::from(inner.height.saturating_sub(1));
    let tasks = &detail.view.tasks;
    let selected = detail.selected();
    let linked = |task: usize| match selected {
        Some(selected) => {
            tasks[selected].needs.contains(&task) || tasks[task].needs.contains(&selected)
        }
        None => false,
    };

    // Each column is as wide as its longest name, plus mark and spacing.
    let widths: Vec<usize> = detail
        .levels
        .iter()
        .map(|level| {
            let longest = level
                .iter()
                .map(|&task| tasks[task].name.chars().count())
                .max();
            longest.unwrap_or(0).clamp(2, LEVEL_NAME_WIDTH) + 5
        })
        .collect();
    // Skip leading levels until the one with the cursor fits on screen.
    let cursor_level = selected.map_or(0, |task| detail.level_of[task]);
    let mut first_level = 0;
    while first_level < cursor_level
        && widths[first_level..=cursor_level].iter().sum::<usize>() > usize::from(inner.width)
    {
        first_level += 1;
    }
    // Scroll every column together so the cursor's row is visible.
    let cursor_row = selected
        .and_then(|task| detail.levels[cursor_level].iter().position(|&t| t == task))
        .unwrap_or(0);
    let first_row = cursor_row.saturating_sub(rows.saturating_sub(1));

    let mut x = inner.x;
    for (level, level_tasks) in detail.levels.iter().enumerate().skip(first_level) {
        let width = u16::try_from(widths[level]).unwrap_or(u16::MAX);
        if x.saturating_add(width) > inner.right() {
            // Levels that do not fit: say so in the corner.
            let more = Rect::new(inner.right().saturating_sub(2), inner.y, 2, 1);
            frame.render_widget(Paragraph::new(" »").style(theme::dim()), more);
            break;
        }
        let mut lines = vec![Line::styled(format!("  L{}", level + 1), theme::dim())];
        for (row, &task) in level_tasks.iter().enumerate().skip(first_row).take(rows) {
            let hidden_below = level_tasks.len() - row - 1;
            if row + 1 == first_row + rows && hidden_below > 0 {
                lines.push(Line::styled(
                    format!("  +{} more", hidden_below + 1),
                    theme::dim(),
                ));
                break;
            }
            let (mark, _, mark_style) = theme::task_state(tasks[task].state);
            let is_selected = selected == Some(task);
            let name_style = if is_selected {
                theme::selected()
            } else if linked(task) {
                Style::new().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else {
                Style::new()
            };
            let name = &tasks[task].name;
            let name = if name.chars().count() > LEVEL_NAME_WIDTH {
                fit(name, LEVEL_NAME_WIDTH)
            } else {
                name.clone()
            };
            lines.push(Line::from(vec![
                Span::styled(if is_selected { CURSOR } else { " " }, theme::bold()),
                Span::styled(mark, mark_style),
                Span::raw(" "),
                Span::styled(name, name_style),
            ]));
        }
        frame.render_widget(
            Paragraph::new(lines),
            Rect::new(x, inner.y, width, inner.height),
        );
        x += width;
    }
}

fn draw_tasks<B: Backend>(app: &App<B>, detail: &Detail, frame: &mut Frame, area: Rect) {
    // cursor, mark, TASK, STATE, TRY, LAST, TIME, MEM, EXIT, WORKER
    let free = usize::from(area.width).saturating_sub(2 + 2 + 10 + 4 + 10 + 8 + 6 + 5 + 9);
    let worker_width = (free / 3).clamp(6, 20);
    let name_width = free.saturating_sub(worker_width);
    let header = format!(
        "  {} {} {} {} {} {} {} {}",
        fit("TASK", name_width),
        fit("STATE", 10),
        fit_right("TRY", 4),
        fit("LAST", 10),
        fit_right("TIME", 8),
        fit_right("MEM", 6),
        fit_right("EXIT", 5),
        fit("WORKER", worker_width),
    );
    let row = |index: usize, selected: bool| {
        let task = &detail.view.tasks[detail.order[index]];
        let (mark, label, style) = theme::task_mark(task);
        let last = task.attempts.last();
        let (outcome, outcome_style) = match last {
            Some(attempt) => theme::outcome(attempt.outcome.as_deref()),
            None => ("", Style::new()),
        };
        let (time, memory, exit) = attempt_figures(task, app.now);
        Line::from(vec![
            cursor_mark(selected),
            Span::styled(format!("{mark} "), style),
            name_cell(&task.name, name_width.saturating_sub(2), selected),
            Span::raw(" "),
            Span::styled(fit(label, 10), style),
            Span::raw(format!(
                " {} ",
                fit_right(&task.attempts.len().to_string(), 4)
            )),
            Span::styled(fit(outcome, 10), outcome_style),
            Span::raw(format!(
                " {} {} {} {}",
                fit_right(&time, 8),
                fit_right(&memory, 6),
                fit_right(&exit, 5),
                fit(
                    last.map_or("", |attempt| attempt.worker.as_str()),
                    worker_width
                ),
            )),
        ])
    };
    let list = List {
        title: "TASKS".to_owned(),
        header,
        cursor: detail.cursor,
        count: detail.order.len(),
        offset: &app.tasks_offset,
        empty: "the run is queued: its tasks appear when it starts",
    };
    draw_list(app, frame, area, &list, row);
}

/// Time, peak memory and exit code of a task's latest attempt.
fn attempt_figures(task: &TaskRow, now: i64) -> (String, String, String) {
    let Some(attempt) = task.attempts.last() else {
        return Default::default();
    };
    let time = match (attempt.started_at, attempt.finished_at) {
        (Some(started), Some(finished)) => theme::elapsed(finished - started),
        (Some(started), None) => theme::elapsed(now - started),
        _ => String::new(),
    };
    let memory = attempt.peak_memory.map(theme::bytes);
    let exit = attempt.exit_code.map(|code| code.to_string());
    (time, memory.unwrap_or_default(), exit.unwrap_or_default())
}

// ---- the whole graph -------------------------------------------------------

/// The GRAPH screen: the state of the run on one line, then its picture
/// with every dependency drawn, scrolled as little as needed to keep the
/// selected task in view. The lines of the selected task are lit, and the
/// tasks at their other end underlined.
fn draw_graph_screen<B: Backend>(app: &App<B>, detail: &Detail, frame: &mut Frame, area: Rect) {
    let [status, rest] = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);
    draw_graph_status(app, detail, frame, status);

    let picture = detail.picture();
    let mut block = theme::window("GRAPH");
    // One cell kept free on the left, where `«` says there is more.
    let frame_inner = block.inner(rest);
    let inner = Rect {
        x: frame_inner.x + 1,
        width: frame_inner.width.saturating_sub(1),
        ..frame_inner
    };
    // The first row of the window numbers the levels; the picture is below.
    let rows = usize::from(inner.height.saturating_sub(1));
    let width = usize::from(inner.width);
    app.page_rows.set(rows.max(1));

    let selected = detail.selected();
    let (mut left, mut top) = app.graph_offset.get();
    if let Some(task) = selected {
        let (row, x) = picture.at[task];
        let end = x + picture.names[task].chars().count() + 2;
        let (start, end) = (x.saturating_sub(GRAPH_MARGIN), end + GRAPH_MARGIN);
        left = keep_in_view(left, start, end, width, picture.width);
        top = keep_in_view(top, row.saturating_sub(1), row + 2, rows, picture.height);
    }
    app.graph_offset.set((left, top));

    // More above or below: the arrows of the old text boxes, on the frame.
    if top > 0 {
        block = block.title_top(Line::styled(" ▲ ", theme::dim()).right_aligned());
    }
    if top + rows < picture.height {
        block = block.title_bottom(Line::styled(" ▼ ", theme::dim()).right_aligned());
    }
    frame.render_widget(block, rest);
    if rows == 0 || width == 0 {
        return;
    }
    let buffer = frame.buffer_mut();
    let first_row = inner.y + 1;
    // A cell of the picture as a cell of the screen. Both are inside the
    // window, whose sides fit in a `u16`.
    let on_screen = |x: usize, y: usize| {
        (
            inner.x + u16::try_from(x).unwrap_or(0),
            first_row + u16::try_from(y).unwrap_or(0),
        )
    };

    let hot: Vec<bool> = picture
        .edges
        .iter()
        .map(|&(need, task)| selected == Some(need) || selected == Some(task))
        .collect();
    let line_style = |is_hot: bool| {
        if is_hot {
            theme::hot_line()
        } else {
            theme::line()
        }
    };
    let window = Window {
        x: left,
        y: top,
        width,
        height: rows,
    };
    for (index, &(bits, is_hot)) in picture.raster(&window, &hot).iter().enumerate() {
        if bits != 0 {
            buffer[on_screen(index % width, index / width)]
                .set_char(graph::glyph(bits))
                .set_style(line_style(is_hot));
        }
    }
    for arrow in &picture.arrows {
        let in_view =
            (top..top + rows).contains(&arrow.row) && (left..left + width).contains(&arrow.x);
        if in_view {
            let is_hot = arrow.edges.iter().any(|&edge| hot[edge as usize]);
            buffer[on_screen(arrow.x - left, arrow.row - top)]
                .set_char('▶')
                .set_style(line_style(is_hot));
        }
    }

    let linked = |task: usize| {
        selected.is_some_and(|selected| {
            picture.needs[selected].contains(&task) || picture.dependents[selected].contains(&task)
        })
    };
    for (task, &(row, x)) in picture.at.iter().enumerate() {
        if !(top..top + rows).contains(&row) {
            continue;
        }
        let y = on_screen(0, row - top).1;
        let (mark, _, mark_style) = theme::task_state(detail.view.tasks[task].state);
        let name_style = if selected == Some(task) {
            theme::selected()
        } else if linked(task) {
            Style::new().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
        } else {
            Style::new()
        };
        put(buffer, inner, left, y, x, mark, mark_style);
        put(
            buffer,
            inner,
            left,
            y,
            x + 2,
            &picture.names[task],
            name_style,
        );
    }

    // The levels over their columns, and what is out of view sideways.
    for (column, &x) in picture.column_x.iter().enumerate() {
        if x >= left {
            let label = format!("L{}", column + 1);
            put(buffer, inner, left, inner.y, x, &label, theme::dim());
        }
    }
    if left > 0 {
        buffer[(frame_inner.x, inner.y)]
            .set_char('«')
            .set_style(theme::dim());
    }
    if left + width < picture.width {
        buffer[(inner.right() - 1, inner.y)]
            .set_char('»')
            .set_style(theme::dim());
    }
}

/// The state of the run on one line, for the GRAPH screen.
fn draw_graph_status<B: Backend>(app: &App<B>, detail: &Detail, frame: &mut Frame, area: Rect) {
    if detail.preview {
        let text = format!(
            " {}: {} on {}, as a new run would start them. t starts one.",
            detail.view.run.dag,
            plural(detail.view.tasks.len(), "task"),
            plural(detail.levels.len(), "level"),
        );
        frame.render_widget(Paragraph::new(text).style(theme::dim()), area);
        return;
    }
    let run = &detail.view.run;
    let (label, style) = theme::run_state(run.state);
    let mut spans = vec![
        Span::styled(format!(" {label}"), style),
        Span::raw(format!("  {}  ", run_time(run, app.now))),
        Span::styled("HP:", theme::bold()),
    ];
    spans.extend(theme::hp_bar(run.tasks, 20));
    spans.push(Span::raw(format!(
        " {}/{}  {}",
        run.tasks.succeeded,
        run.tasks.total,
        level_reached(detail)
    )));
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Writes `text` from cell `x` of the picture on screen row `y`, leaving
/// out what falls outside the window, which starts at cell `left`.
fn put(buffer: &mut Buffer, inner: Rect, left: usize, y: u16, x: usize, text: &str, style: Style) {
    for (offset, character) in text.chars().enumerate() {
        let Some(column) = (x + offset).checked_sub(left) else {
            continue;
        };
        if column >= usize::from(inner.width) {
            break;
        }
        buffer[(inner.x + u16::try_from(column).unwrap_or(0), y)]
            .set_char(character)
            .set_style(style);
    }
}

/// Where a window `size` long over something `total` long starts, moved
/// from `offset` as little as needed to show `start..end`, or at least its
/// start when it does not fit.
fn keep_in_view(offset: usize, start: usize, end: usize, size: usize, total: usize) -> usize {
    let mut offset = offset;
    if end > offset + size {
        offset = end - size;
    }
    if start < offset {
        offset = start;
    }
    offset.min(total.saturating_sub(size))
}

// ---- output of a task ------------------------------------------------------

fn draw_log<B: Backend>(app: &App<B>, log: &LogView, frame: &mut Frame, area: Rect) {
    let stream = |which: Show| {
        let style = if log.show == which {
            theme::selected()
        } else {
            theme::dim()
        };
        Span::styled(format!(" {} ", which.name()), style)
    };
    let name = match (&log.task, log.attempts.get(log.attempt)) {
        (Some(task), Some(number)) => format!(" {task} #{number} "),
        _ => " ALL TASKS ".to_owned(),
    };
    let title = Line::from(vec![
        Span::styled(name, theme::bold()),
        stream(Show::All),
        stream(Show::Out),
        stream(Show::Err),
        Span::raw(" "),
    ]);

    let lines: Vec<_> = log.visible(&app.filter).collect();
    let position = if log.scroll_up == 0 {
        " following ".to_owned()
    } else {
        format!(" {} lines below ", log.scroll_up)
    };
    let mut block = theme::window("")
        .title(title)
        .title_bottom(Line::styled(position, theme::dim()).right_aligned());
    if !app.filter.is_empty() {
        let search = format!(" / {} ", app.filter);
        block =
            block.title_top(Line::styled(search, Style::new().fg(Color::Yellow)).right_aligned());
    }
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let rows = usize::from(inner.height);
    app.page_rows.set(rows);

    if lines.is_empty() {
        let empty = if log.lines.is_empty() {
            " (nothing was written yet)"
        } else {
            " (no line matches)"
        };
        frame.render_widget(Paragraph::new(empty).style(theme::dim()), inner);
        return;
    }
    let end = lines
        .len()
        .saturating_sub(log.scroll_up)
        .max(rows.min(lines.len()));
    let start = end.saturating_sub(rows);
    let width = usize::from(inner.width);
    let several = log.names.len() > 1;
    let name_width = log
        .names
        .iter()
        .map(|name| name.chars().count())
        .max()
        .unwrap_or(0)
        .min(16);
    let text: Vec<Line> = lines[start..end]
        .iter()
        .map(|(source, line)| {
            let style = match line.stream {
                'E' => Style::new().fg(Color::Red),
                'S' => Style::new().fg(Color::Cyan),
                _ => Style::new(),
            };
            let time = match line.at {
                0 => "        ".to_owned(),
                at => crate::util::format_local(at)
                    .get(11..)
                    .unwrap_or_default()
                    .to_owned(),
            };
            let mut spans = vec![Span::styled(format!(" {time} "), theme::dim())];
            if several {
                // Which task says it, as `stern` does with pods.
                spans.push(Span::styled(
                    format!("{} ", fit(&log.names[*source], name_width)),
                    Style::new().fg(Color::Blue),
                ));
            }
            let clean: String = printable(&line.text)
                .chars()
                .skip(log.scroll_right)
                .take(width)
                .collect();
            spans.push(Span::styled(clean, style));
            Line::from(spans)
        })
        .collect();
    frame.render_widget(Paragraph::new(text), inner);
}

/// A line of command output made safe to show: colour and cursor escape
/// sequences are removed (they would act on our own screen), tabs become
/// spaces, and only what follows the last carriage return is kept, as a
/// terminal would show for a progress bar.
pub(crate) fn printable(line: &str) -> String {
    let line = line.trim_end_matches('\r');
    let line = line.rsplit('\r').next().unwrap_or(line);
    let mut clean = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => match chars.peek() {
                // CSI: ESC [ parameters, ended by a letter or similar.
                Some('[') => {
                    chars.next();
                    for c in chars.by_ref() {
                        if ('\u{40}'..='\u{7e}').contains(&c) {
                            break;
                        }
                    }
                }
                // OSC: ESC ] text, ended by BEL or ESC \.
                Some(']') => {
                    chars.next();
                    while let Some(c) = chars.next() {
                        if c == '\u{7}' || (c == '\u{1b}' && chars.next_if_eq(&'\\').is_some()) {
                            break;
                        }
                    }
                }
                // Any other escape is two characters long.
                _ => {
                    chars.next();
                }
            },
            '\t' => clean.push_str("    "),
            c if c.is_control() => {}
            c => clean.push(c),
        }
    }
    clean
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn printable_removes_what_would_disturb_the_screen() {
        assert_eq!(printable("plain text"), "plain text");
        assert_eq!(printable("\u{1b}[1;31merror\u{1b}[0m: bad"), "error: bad");
        assert_eq!(printable("a\tb"), "a    b");
        assert_eq!(printable("10%\r50%\r100%"), "100%");
        assert_eq!(printable("windows line\r"), "windows line");
        assert_eq!(printable("\u{1b}]0;title\u{7}after"), "after");
        assert_eq!(printable("\u{1b}]8;;http://x\u{1b}\\link"), "link");
        assert_eq!(printable("bell\u{7} and null\u{0}"), "bell and null");
        assert_eq!(printable("accenti: però ✓"), "accenti: però ✓");
        assert_eq!(printable("cut \u{1b}["), "cut ");
    }

    #[test]
    fn visible_rows_scroll_only_at_the_edges() {
        let offset = Cell::new(0);
        assert_eq!(visible_rows(0, 10, 4, &offset), 0..4);
        assert_eq!(visible_rows(3, 10, 4, &offset), 0..4, "still in view");
        assert_eq!(visible_rows(4, 10, 4, &offset), 1..5, "one step down");
        assert_eq!(
            visible_rows(3, 10, 4, &offset),
            1..5,
            "going back up: no jump"
        );
        assert_eq!(visible_rows(0, 10, 4, &offset), 0..4);
        assert_eq!(visible_rows(9, 10, 4, &offset), 6..10);
        // The list got shorter: the window follows.
        assert_eq!(visible_rows(2, 3, 4, &offset), 0..3);
        assert_eq!(visible_rows(0, 0, 4, &offset), 0..0);
        assert!(
            visible_rows(5, 10, 0, &offset).is_empty(),
            "no room, no rows"
        );
    }
}
