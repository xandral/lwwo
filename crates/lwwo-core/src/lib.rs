//! Decision core of lwwo.
//!
//! It receives events and the current time as inputs and returns commands.
//! It never reads the clock, the database, the network, the environment or
//! files, which keeps it deterministic and easy to simulate.

pub mod calendar;
pub mod cron;
pub mod dag;
pub mod deadlines;
pub mod engine;
pub mod ready_queue;
pub mod run;
pub mod time;
pub mod timetable;
pub mod worker_selection;
