//! Decision core of lwwo.
//!
//! It receives events and the current time as inputs and returns commands.
//! It never reads the clock, the database, the network, the environment or
//! files, which keeps it deterministic and easy to simulate.

pub mod dag;
pub mod ready_queue;
pub mod run;
pub mod worker_selection;
