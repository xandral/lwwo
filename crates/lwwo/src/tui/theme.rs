//! The look: frames, colours, glyphs and the small formatting helpers shared
//! by every screen. The style borrows from two places: the layout and key
//! hints of k9s, and the bordered message boxes, `▶` cursors and HP bars of
//! the first Pokémon games.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType};

use super::backend::{Counts, RunState, TaskState};

/// The cursor of every list and menu.
pub(crate) const CURSOR: &str = "▶";
/// Shown in the message box while more messages are waiting.
pub(crate) const MORE: &str = "▼";

/// A window with the double border of a Game Boy text box. An empty title
/// leaves the border whole.
pub(crate) fn window(title: &str) -> Block<'static> {
    let block = Block::bordered().border_type(BorderType::Double);
    if title.is_empty() {
        block
    } else {
        block.title(Line::from(format!(" {title} ")).style(bold()))
    }
}

pub(crate) fn bold() -> Style {
    Style::new().add_modifier(Modifier::BOLD)
}

pub(crate) fn dim() -> Style {
    Style::new().fg(Color::DarkGray)
}

/// The style of the row or item under the cursor.
pub(crate) fn selected() -> Style {
    Style::new().add_modifier(Modifier::BOLD | Modifier::REVERSED)
}

/// The lines of the GRAPH screen.
pub(crate) fn line() -> Style {
    dim()
}

/// The lines of the dependencies of the selected task.
pub(crate) fn hot_line() -> Style {
    Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}

/// Label and colour of a run state.
pub(crate) fn run_state(state: RunState) -> (&'static str, Style) {
    match state {
        RunState::Queued => ("QUEUED", Style::new().fg(Color::Cyan)),
        RunState::Running => ("RUNNING", Style::new().fg(Color::Yellow)),
        RunState::Success => ("SUCCESS", Style::new().fg(Color::Green)),
        RunState::Failed => ("FAILED", Style::new().fg(Color::Red)),
        RunState::Cancelled => ("CANCELLED", Style::new().fg(Color::Magenta)),
    }
}

/// One-character mark, label and colour of a task state.
pub(crate) fn task_state(state: TaskState) -> (&'static str, &'static str, Style) {
    match state {
        TaskState::Blocked => ("·", "WAITING", dim()),
        TaskState::Assigned => ("●", "RUNNING", Style::new().fg(Color::Yellow)),
        TaskState::Retrying => ("↻", "RETRYING", Style::new().fg(Color::Cyan)),
        TaskState::Success => ("✓", "SUCCESS", Style::new().fg(Color::Green)),
        TaskState::Failed => ("✗", "FAILED", Style::new().fg(Color::Red)),
        TaskState::Skipped => ("-", "SKIPPED", dim()),
        TaskState::Cancelled => ("×", "CANCELLED", Style::new().fg(Color::Magenta)),
    }
}

/// Like [`task_state`], for a task in a table: a task that is ready but
/// finds no worker is told apart from one that waits for what it needs.
pub(crate) fn task_mark(task: &super::backend::TaskRow) -> (&'static str, &'static str, Style) {
    if task.waiting.is_some() && task.state == TaskState::Blocked {
        ("○", "READY", Style::new().fg(Color::Cyan))
    } else {
        task_state(task.state)
    }
}

/// Label and colour of the outcome of an attempt.
pub(crate) fn outcome(outcome: Option<&str>) -> (&'static str, Style) {
    match outcome {
        None => ("running", Style::new().fg(Color::Yellow)),
        Some("success") => ("success", Style::new().fg(Color::Green)),
        Some("failed") => ("failed", Style::new().fg(Color::Red)),
        Some("timeout") => ("timeout", Style::new().fg(Color::Red)),
        Some("lost") => ("lost", Style::new().fg(Color::Magenta)),
        Some("cancelled") => ("cancelled", Style::new().fg(Color::Magenta)),
        Some(_) => ("?", dim()),
    }
}

/// An HP bar for a run: green for the tasks that succeeded, red for those
/// that failed, empty for what is left.
pub(crate) fn hp_bar(counts: Counts, width: usize) -> Vec<Span<'static>> {
    let total = counts.total.max(1) as usize;
    // Round to the nearest cell, but never show a non-zero count as nothing.
    let cells = |count: u32| {
        let count = count as usize;
        if count == 0 {
            0
        } else {
            ((count * width + total / 2) / total).max(1)
        }
    };
    let failed = cells(counts.failed).min(width);
    let succeeded = cells(counts.succeeded).min(width - failed);
    let empty = width - failed - succeeded;
    vec![
        Span::styled("█".repeat(succeeded), Style::new().fg(Color::Green)),
        Span::styled("█".repeat(failed), Style::new().fg(Color::Red)),
        Span::styled("░".repeat(empty), dim()),
    ]
}

/// Fits `text` in exactly `width` columns: padded with spaces on the right,
/// or cut with an ellipsis.
pub(crate) fn fit(text: &str, width: usize) -> String {
    let length = text.chars().count();
    if length <= width {
        format!("{text}{}", " ".repeat(width - length))
    } else if width == 0 {
        String::new()
    } else {
        let kept: String = text.chars().take(width - 1).collect();
        format!("{kept}…")
    }
}

/// Like [`fit`], but cuts the middle and keeps the end. Run ids start with
/// the workflow name and end with what tells them apart, so the end is the
/// part worth keeping.
pub(crate) fn fit_middle(text: &str, width: usize) -> String {
    let length = text.chars().count();
    if length <= width || width < 4 {
        return fit(text, width);
    }
    let tail = 8.min(width - 2);
    let head = width - 1 - tail;
    let start: String = text.chars().take(head).collect();
    let end: String = text.chars().skip(length - tail).collect();
    format!("{start}…{end}")
}

/// Like [`fit`], aligned to the right: for numbers.
pub(crate) fn fit_right(text: &str, width: usize) -> String {
    let length = text.chars().count();
    if length <= width {
        format!("{}{text}", " ".repeat(width - length))
    } else {
        fit(text, width)
    }
}

/// A span of time as one short word: `42s`, `3m`, `5h`, `2d`.
pub(crate) fn age(millis: i64) -> String {
    let seconds = millis.max(0) / 1000;
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86_400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// A duration with the precision that suits its size: `0.4s`, `42s`,
/// `3:07`, `1:02:45`.
pub(crate) fn elapsed(millis: i64) -> String {
    let millis = millis.max(0);
    let seconds = millis / 1000;
    match seconds {
        0..10 => format!("{}.{}s", seconds, millis % 1000 / 100),
        10..60 => format!("{seconds}s"),
        60..3600 => format!("{}:{:02}", seconds / 60, seconds % 60),
        _ => format!(
            "{}:{:02}:{:02}",
            seconds / 3600,
            seconds % 3600 / 60,
            seconds % 60
        ),
    }
}

/// An instant as the hour of today, or as a date if it is another day.
pub(crate) fn when(millis: i64, now: i64) -> String {
    let full = crate::util::format_local(millis);
    let today = crate::util::format_local(now);
    match (full.get(..10), today.get(..10)) {
        (Some(day), Some(this_day)) if day == this_day => full[11..].to_owned(),
        _ => full.get(5..16).unwrap_or(&full).to_owned(),
    }
}

/// A size in bytes with a binary unit: `512K`, `34M`, `1.5G`.
pub(crate) fn bytes(bytes: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;
    match bytes {
        0..KIB => format!("{bytes}B"),
        KIB..MIB => format!("{}K", bytes / KIB),
        MIB..GIB => format!("{}M", bytes / MIB),
        _ => format!("{:.1}G", bytes as f64 / GIB as f64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(succeeded: u32, failed: u32, total: u32, width: usize) -> String {
        let counts = Counts {
            total,
            succeeded,
            failed,
            active: 0,
        };
        hp_bar(counts, width)
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    #[test]
    fn hp_bar_always_has_the_requested_width() {
        assert_eq!(bar(0, 0, 4, 8), "░░░░░░░░");
        assert_eq!(bar(2, 0, 4, 8), "████░░░░");
        assert_eq!(bar(4, 0, 4, 8), "████████");
        assert_eq!(bar(3, 1, 4, 8), "████████");
        assert_eq!(bar(0, 0, 0, 5), "░░░░░");
        // One task out of a thousand is still visible, and nothing overflows.
        assert_eq!(bar(1, 0, 1000, 10), "█░░░░░░░░░");
        assert_eq!(bar(999, 1, 1000, 10), "██████████");
        for (succeeded, failed) in [(0, 7), (7, 0), (3, 4), (1, 1), (5, 2)] {
            assert_eq!(bar(succeeded, failed, 7, 9).chars().count(), 9);
        }
    }

    #[test]
    fn fit_pads_or_cuts_to_the_exact_width() {
        assert_eq!(fit("ab", 4), "ab  ");
        assert_eq!(fit("abcd", 4), "abcd");
        assert_eq!(fit("abcdef", 4), "abc…");
        assert_eq!(fit("abc", 0), "");
        assert_eq!(fit_middle("short", 8), "short   ");
        assert_eq!(fit_middle("etl-giornaliero-mv1a29uw", 13), "etl-…mv1a29uw");
        assert_eq!(fit_middle("etl-giornaliero-mv1a29uw", 5), "e…9uw");
        assert_eq!(fit_middle("abcdef", 3), "ab…");
        assert_eq!(fit_right("7", 3), "  7");
        assert_eq!(fit_right("12345", 3), "12…");
    }

    #[test]
    fn times_and_sizes_are_short() {
        assert_eq!(age(999), "0s");
        assert_eq!(age(61_000), "1m");
        assert_eq!(age(3 * 3_600_000), "3h");
        assert_eq!(age(49 * 3_600_000), "2d");
        assert_eq!(elapsed(450), "0.4s");
        assert_eq!(elapsed(42_000), "42s");
        assert_eq!(elapsed(187_000), "3:07");
        assert_eq!(elapsed(3_765_000), "1:02:45");
        assert_eq!(bytes(900), "900B");
        assert_eq!(bytes(34 << 20), "34M");
        assert_eq!(bytes(3 << 29), "1.5G");
    }
}
