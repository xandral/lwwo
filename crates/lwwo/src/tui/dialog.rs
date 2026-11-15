//! The message box at the bottom of the screen.
//!
//! News are typed out one character at a time, as in the text boxes of old
//! handheld games, stay for a moment and make room for the next. When there
//! is no news the box describes what is under the cursor instead.

use std::collections::VecDeque;

/// Milliseconds per character while a message is typed out.
const PER_CHARACTER: i64 = 14;
/// How long a message stays once it is all there (ms).
const STAY: i64 = 3_500;
/// The same when other messages are waiting.
const STAY_WHEN_BUSY: i64 = 1_200;
/// Messages kept waiting: older ones are dropped rather than shown late.
const WAITING: usize = 5;

#[derive(Debug, Default)]
pub(crate) struct Dialog {
    queue: VecDeque<String>,
    /// The message on display and when it started (ms).
    current: Option<(String, i64)>,
    now: i64,
}

impl Dialog {
    /// Adds a message to show.
    pub(crate) fn push(&mut self, text: impl Into<String>) {
        if self.queue.len() == WAITING {
            self.queue.pop_front();
        }
        self.queue.push_back(text.into());
    }

    fn typing_time(text: &str) -> i64 {
        i64::try_from(text.chars().count()).unwrap_or(i64::MAX) * PER_CHARACTER
    }

    /// Moves time forward: the next message comes when this one had its
    /// time.
    pub(crate) fn tick(&mut self, now: i64) {
        self.now = now;
        if let Some((text, started)) = &self.current {
            let stay = if self.queue.is_empty() {
                STAY
            } else {
                STAY_WHEN_BUSY
            };
            if now - started >= Self::typing_time(text) + stay {
                self.current = None;
            }
        }
        if self.current.is_none() {
            self.current = self.queue.pop_front().map(|text| (text, now));
        }
    }

    /// True while a message is still being typed out: the screen is then
    /// drawn more often.
    pub(crate) fn is_typing(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|(text, started)| self.now - started < Self::typing_time(text))
    }

    /// The space bar: shows the whole message at once, or, if it is all
    /// there already, dismisses it.
    pub(crate) fn skip(&mut self, now: i64) {
        if self.is_typing() {
            if let Some((text, started)) = &mut self.current {
                *started = now - Self::typing_time(text);
            }
        } else {
            self.current = None;
            self.tick(now);
        }
    }

    /// Messages waiting behind the one on display.
    pub(crate) fn waiting(&self) -> usize {
        self.queue.len()
    }

    /// The lines of the message on display, as far as it was typed; `None`
    /// when there is no message.
    pub(crate) fn lines(&self, width: usize, rows: usize) -> Option<Vec<String>> {
        let (text, started) = self.current.as_ref()?;
        let typed = usize::try_from((self.now - started) / PER_CHARACTER).unwrap_or(0);
        // Wrap the whole text first, so that words do not jump to the next
        // line while they are typed.
        let mut left = typed;
        let mut lines = Vec::new();
        for line in wrap(text, width, rows) {
            let shown: String = line.chars().take(left).collect();
            left = left.saturating_sub(line.chars().count() + 1);
            lines.push(shown);
        }
        Some(lines)
    }
}

/// Breaks `text` into at most `rows` lines of at most `width` characters,
/// at spaces where it can. What does not fit ends with an ellipsis.
pub(crate) fn wrap(text: &str, width: usize, rows: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    if width == 0 || rows == 0 {
        return lines;
    }
    for word in text.split_whitespace() {
        let mut word: String = word.to_owned();
        loop {
            let length = word.chars().count();
            let fits = match lines.last() {
                Some(line) => line.chars().count() + 1 + length <= width,
                None => false,
            };
            if fits {
                let line = lines.last_mut().expect("there is a line");
                line.push(' ');
                line.push_str(&word);
                break;
            }
            if length <= width {
                lines.push(word);
                break;
            }
            // A word longer than a line is cut where the line ends.
            let head: String = word.chars().take(width).collect();
            word = word.chars().skip(width).collect();
            lines.push(head);
        }
    }
    if lines.len() > rows {
        lines.truncate(rows);
        if let Some(last) = lines.last_mut() {
            let kept: String = last.chars().take(width.saturating_sub(1)).collect();
            *last = format!("{kept}…");
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_breaks_at_spaces_and_cuts_what_does_not_fit() {
        assert_eq!(wrap("one two three", 7, 3), ["one two", "three"]);
        assert_eq!(wrap("abcdefghij", 4, 3), ["abcd", "efgh", "ij"]);
        assert_eq!(wrap("one two three four", 7, 2), ["one two", "three…"]);
        assert!(wrap("anything", 0, 3).is_empty());
        assert!(wrap("", 10, 3).is_empty());
    }

    #[test]
    fn a_message_is_typed_out_stays_and_goes() {
        let mut dialog = Dialog::default();
        dialog.push("Hello there");
        dialog.tick(1_000);
        assert!(dialog.is_typing());
        assert_eq!(dialog.lines(20, 2).unwrap(), [""]);
        dialog.tick(1_000 + 5 * PER_CHARACTER);
        assert_eq!(dialog.lines(20, 2).unwrap(), ["Hello"]);
        dialog.tick(1_000 + 11 * PER_CHARACTER);
        assert!(!dialog.is_typing());
        assert_eq!(dialog.lines(20, 2).unwrap(), ["Hello there"]);
        dialog.tick(1_000 + 11 * PER_CHARACTER + STAY);
        assert_eq!(dialog.lines(20, 2), None);
    }

    #[test]
    fn space_finishes_the_message_then_moves_on() {
        let mut dialog = Dialog::default();
        dialog.push("First message");
        dialog.push("Second");
        dialog.tick(0);
        assert_eq!(dialog.waiting(), 1);
        dialog.skip(10);
        dialog.tick(10);
        assert_eq!(dialog.lines(20, 1).unwrap(), ["First message"]);
        dialog.skip(20);
        assert_eq!(dialog.waiting(), 0);
        dialog.tick(20 + 6 * PER_CHARACTER);
        assert_eq!(dialog.lines(20, 1).unwrap(), ["Second"]);
    }

    #[test]
    fn old_news_are_dropped_rather_than_shown_late() {
        let mut dialog = Dialog::default();
        for number in 0..20 {
            dialog.push(format!("message {number}"));
        }
        assert_eq!(dialog.waiting(), WAITING);
        dialog.tick(0);
        dialog.skip(0);
        dialog.tick(0);
        assert_eq!(dialog.lines(20, 1).unwrap(), ["message 15"]);
    }
}
