//! The terminal interface of lwwo, in the style of k9s.
//!
//! Three layers, each ignorant of the next one's details:
//!
//! - [`Backend`] is where the data comes from and where actions go: the
//!   master's HTTP API, or made-up data for `--demo` and the tests;
//! - [`App`] is the state of the interface and how keys change it;
//! - [`ui::draw`] turns that state into a screen.
//!
//! This module ties them to a real terminal. The TUI is a client like the
//! command line: it never reads SQLite, so it works with a remote master.

mod app;
mod backend;
mod demo;
mod dialog;
mod graph;
mod remote;
mod theme;
mod ui;

#[cfg(test)]
mod tests;

use std::io;
use std::time::Duration;

use anyhow::Result;
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event, KeyEventKind};

use self::app::App;
use self::backend::Backend;
use crate::client::Client;
use crate::util::now_ms;

/// Pause between two drawings while text is being typed out.
const FRAME: Duration = Duration::from_millis(33);
/// Pause between two drawings when nothing moves: a key press ends it early.
const IDLE: Duration = Duration::from_millis(250);

/// Takes over the terminal and runs the interface until the user quits.
///
/// The terminal is put back as it was on the way out, also after an error
/// or a panic (ratatui installs a hook for that).
fn run_with(backend: impl Backend) -> io::Result<()> {
    let mut terminal = ratatui::try_init()?;
    let result = event_loop(&mut terminal, App::new(backend, now_ms()));
    ratatui::restore();
    result
}

fn event_loop<B: Backend>(terminal: &mut DefaultTerminal, mut app: App<B>) -> io::Result<()> {
    loop {
        app.on_tick(now_ms());
        if app.should_quit() {
            return Ok(());
        }
        terminal.draw(|frame| ui::draw(&app, frame))?;

        let wait = if app.is_animating() { FRAME } else { IDLE };
        if event::poll(wait)? {
            // Handle every key already typed before drawing again.
            loop {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        app.on_key(key);
                    }
                }
                if !event::poll(Duration::ZERO)? {
                    break;
                }
            }
        }
    }
}

/// Runs the TUI against a master until the user quits.
pub(crate) async fn run(client: Client) -> Result<()> {
    // Fail with a plain message, before taking over the terminal, if the
    // master is not there.
    client.get("/status").await?;
    let runtime = tokio::runtime::Handle::current();
    // The interface is a blocking loop: step aside from the async runtime,
    // which goes on serving the event and log streams.
    tokio::task::block_in_place(|| run_with(remote::Remote::new(client, runtime)))?;
    Ok(())
}

/// Runs the TUI on made-up data: to look at it without a master.
pub(crate) fn run_demo() -> Result<()> {
    run_with(demo::Demo::new(now_ms()))?;
    Ok(())
}
