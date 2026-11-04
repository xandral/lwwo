//! When each DAG fires, and what to do when it is due.
//!
//! The timetable itself lives in the database, one row per DAG with its
//! next fire. This module holds the rules around that row:
//!
//! - **Next fire.** The cron is read in the DAG's time zone. A time the
//!   clocks jump over fires at the first valid instant after it; a time
//!   that happens twice fires only the first time.
//! - **Catch-up.** After a gap, `catchup: false` starts only the latest
//!   missed fire; `catchup: true` starts every one, in order, one at a time.
//! - **Overlap.** What to do when a fire comes while runs of the same DAG
//!   are still active.
//!
//! When a fire has to wait, the row stays behind, in the past, and the
//! decision is taken again when an active run finishes:
//!
//! ```text
//! overlap: queue, hourly
//! 03:00  run A starts            next fire 04:00
//! 04:00  A still running: Wait   next fire stays 04:00
//! 04:20  A finishes: Start the 04:00 run, late, as `catchup`
//! ```

use crate::cron::Cron;
use crate::time::{LocalTime, TimeZone, Timestamp};

/// A run that starts more than this many milliseconds after its fire is a
/// catch-up run.
pub const CATCHUP_TOLERANCE_MS: i64 = 60_000;

/// What to do when a fire comes while runs of the same DAG are active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overlap {
    /// Wait until no run is active.
    Queue,
    /// Skip the fire, and record it as skipped.
    Skip,
    /// Start alongside, up to this many active runs; beyond that, wait.
    Allow {
        /// The most runs of the DAG active at once.
        max_active: u32,
    },
    /// Cancel the active runs and start the new one.
    Replace,
}

/// Why a run was created by the timetable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// On time.
    Cron,
    /// More than [`CATCHUP_TOLERANCE_MS`] after its fire.
    Catchup,
}

/// What to do with a DAG whose row in the timetable is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Nothing yet: the fire is in the future, or the overlap rule makes it
    /// wait. The row stays as it is.
    Wait,
    /// Create a run.
    Start {
        /// The instant of the fire, which the run sees as its time.
        logical_time: Timestamp,
        /// On time or late.
        trigger: Trigger,
        /// The new value of the row, or `None` to remove it: the cron never
        /// fires again.
        next_fire: Option<Timestamp>,
        /// Cancel the active runs first (`overlap: replace`).
        cancel_active: bool,
    },
    /// Record the fire as skipped, without a run.
    Skip {
        /// The instant of the skipped fire.
        logical_time: Timestamp,
        /// The new value of the row, or `None` to remove it.
        next_fire: Option<Timestamp>,
    },
}

/// The schedule of a DAG: its cron, its time zone and its rules.
#[derive(Debug, Clone)]
pub struct Schedule<Z> {
    cron: Cron,
    zone: Z,
    catchup: bool,
    overlap: Overlap,
}

impl<Z: TimeZone> Schedule<Z> {
    /// The schedule that reads `cron` in `zone`.
    #[must_use]
    pub const fn new(cron: Cron, zone: Z, catchup: bool, overlap: Overlap) -> Self {
        Self {
            cron,
            zone,
            catchup,
            overlap,
        }
    }

    /// The first fire strictly after `after`, or `None` if the cron never
    /// fires again.
    #[must_use]
    pub fn next_fire(&self, after: Timestamp) -> Option<Timestamp> {
        let mut local = self.zone.local(after);
        loop {
            local = self.cron.next_after(local)?;
            let instant = match self.zone.resolve(local) {
                LocalTime::Unique(instant) | LocalTime::Repeated { first: instant, .. } => instant,
                LocalTime::Skipped { resume } => resume,
            };
            // Near a clock change a later wall-clock time can map to an
            // instant that is not later: a jump folds several times onto
            // the same instant, and a repeated time comes back. Those
            // fires already happened, so keep looking.
            if instant > after {
                return Some(instant);
            }
        }
    }

    /// The latest fire at or before `now`, counting from `first`, a fire
    /// that is already due.
    ///
    /// ```text
    /// hourly, first 00:00, now 03:30  →  03:00
    /// hourly, first 00:00, now 00:00  →  00:00
    /// ```
    fn latest_due(&self, first: Timestamp, now: Timestamp) -> Timestamp {
        let mut latest = first;
        while let Some(next) = self.next_fire(latest) {
            if next > now {
                break;
            }
            latest = next;
        }
        latest
    }

    /// What to do now with a DAG whose row says `next_fire`, while
    /// `active_runs` of its runs are not finished.
    ///
    /// Call it when the row is due, and again whenever a run of the DAG
    /// finishes; with `catchup: true` or `overlap: allow`, keep calling it
    /// until it says [`Decision::Wait`], since several runs may start.
    #[must_use]
    pub fn decide(&self, next_fire: Timestamp, now: Timestamp, active_runs: u32) -> Decision {
        if next_fire > now {
            return Decision::Wait;
        }

        let logical_time = if self.catchup {
            next_fire
        } else {
            self.latest_due(next_fire, now)
        };
        let following = self.next_fire(logical_time);
        let trigger = if now.as_millis() - logical_time.as_millis() > CATCHUP_TOLERANCE_MS {
            Trigger::Catchup
        } else {
            Trigger::Cron
        };
        let start = |cancel_active| Decision::Start {
            logical_time,
            trigger,
            next_fire: following,
            cancel_active,
        };

        // The overlap rule only matters while runs are active.
        if active_runs == 0 {
            return start(false);
        }
        match self.overlap {
            Overlap::Allow { max_active } if active_runs < max_active => start(false),
            Overlap::Queue | Overlap::Allow { .. } => Decision::Wait,
            Overlap::Skip => Decision::Skip {
                logical_time,
                next_fire: following,
            },
            Overlap::Replace => start(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar::{Date, DateTime};
    use crate::time::Utc;

    const MINUTE: i64 = 60_000;
    const HOUR: i64 = 60 * MINUTE;

    fn local(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> DateTime {
        let date = Date::new(year, month, day).expect("valid date in test");
        DateTime::new(date, hour, minute).expect("valid time in test")
    }

    fn utc(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> Timestamp {
        Timestamp::from_local(local(year, month, day, hour, minute), 0)
    }

    fn plus(t: Timestamp, millis: i64) -> Timestamp {
        Timestamp::from_millis(t.as_millis() + millis)
    }

    /// A zone with a single clock change at `at`, from `before` to `after`
    /// milliseconds ahead of UTC.
    struct OneChange {
        at: Timestamp,
        before: i64,
        after: i64,
    }

    impl TimeZone for OneChange {
        fn local(&self, instant: Timestamp) -> DateTime {
            let offset = if instant < self.at {
                self.before
            } else {
                self.after
            };
            instant.to_local(offset)
        }

        fn resolve(&self, local: DateTime) -> LocalTime {
            let early = Timestamp::from_local(local, self.before);
            let late = Timestamp::from_local(local, self.after);
            match (early < self.at, late >= self.at) {
                (true, true) => LocalTime::Repeated {
                    first: early.min(late),
                    second: early.max(late),
                },
                (true, false) => LocalTime::Unique(early),
                (false, true) => LocalTime::Unique(late),
                (false, false) => LocalTime::Skipped { resume: self.at },
            }
        }
    }

    /// Rome on 2026-03-29: at 01:00 UTC the clocks go from 02:00 to 03:00.
    fn rome_spring() -> OneChange {
        OneChange {
            at: utc(2026, 3, 29, 1, 0),
            before: HOUR,
            after: 2 * HOUR,
        }
    }

    /// Rome on 2026-10-25: at 01:00 UTC the clocks go from 03:00 to 02:00.
    fn rome_autumn() -> OneChange {
        OneChange {
            at: utc(2026, 10, 25, 1, 0),
            before: 2 * HOUR,
            after: HOUR,
        }
    }

    fn schedule<Z: TimeZone>(cron: &str, zone: Z, catchup: bool, overlap: Overlap) -> Schedule<Z> {
        let cron = cron.parse().expect("valid cron in test");
        Schedule::new(cron, zone, catchup, overlap)
    }

    fn hourly(catchup: bool, overlap: Overlap) -> Schedule<Utc> {
        schedule("0 * * * *", Utc, catchup, overlap)
    }

    // Already working

    #[test]
    fn next_fire_in_utc() {
        let daily = schedule("0 3 * * *", Utc, false, Overlap::Queue);
        assert_eq!(
            daily.next_fire(utc(2026, 11, 3, 2, 59)),
            Some(utc(2026, 11, 3, 3, 0))
        );
        assert_eq!(
            daily.next_fire(utc(2026, 11, 3, 3, 0)),
            Some(utc(2026, 11, 4, 3, 0))
        );
    }

    #[test]
    fn next_fire_reads_the_cron_in_the_zone() {
        // 03:00 in Rome in winter is 02:00 UTC.
        let zone = OneChange {
            at: utc(2000, 1, 1, 0, 0),
            before: HOUR,
            after: HOUR,
        };
        let daily = schedule("0 3 * * *", zone, false, Overlap::Queue);
        assert_eq!(
            daily.next_fire(utc(2026, 11, 3, 0, 0)),
            Some(utc(2026, 11, 3, 2, 0))
        );
    }

    #[test]
    fn a_skipped_time_fires_when_the_clocks_jump() {
        // 02:30 does not exist on 2026-03-29: it fires at 03:00 local.
        let daily = schedule("30 2 * * *", rome_spring(), false, Overlap::Queue);
        assert_eq!(
            daily.next_fire(utc(2026, 3, 28, 12, 0)),
            Some(utc(2026, 3, 29, 1, 0))
        );
    }

    #[test]
    fn times_folded_by_a_jump_fire_once() {
        // 02:00, 02:15, 02:30 and 02:45 all fold onto 03:00 local.
        let quarters = schedule("*/15 * * * *", rome_spring(), false, Overlap::Queue);
        let before = utc(2026, 3, 29, 0, 45); // 01:45 local
        let jump = quarters.next_fire(before).expect("fires");
        assert_eq!(jump, utc(2026, 3, 29, 1, 0)); // 03:00 local
        assert_eq!(quarters.next_fire(jump), Some(utc(2026, 3, 29, 1, 15)));
    }

    #[test]
    fn a_repeated_time_fires_only_the_first_time() {
        // 02:30 happens twice on 2026-10-25: at 00:30 and 01:30 UTC.
        let daily = schedule("30 2 * * *", rome_autumn(), false, Overlap::Queue);
        let first = daily.next_fire(utc(2026, 10, 24, 12, 0)).expect("fires");
        assert_eq!(first, utc(2026, 10, 25, 0, 30));
        assert_eq!(daily.next_fire(first), Some(utc(2026, 10, 26, 1, 30)));
    }

    // TASK 19

    #[test]
    fn latest_due_takes_the_last_fire_before_now() {
        let s = hourly(false, Overlap::Queue);
        let first = utc(2026, 11, 3, 0, 0);
        assert_eq!(
            s.latest_due(first, utc(2026, 11, 3, 3, 30)),
            utc(2026, 11, 3, 3, 0)
        );
        assert_eq!(
            s.latest_due(first, utc(2026, 11, 3, 3, 0)),
            utc(2026, 11, 3, 3, 0)
        );
        assert_eq!(s.latest_due(first, first), first);
    }

    // TASK 20 (needs 19)

    #[test]
    fn a_fire_in_the_future_waits() {
        let s = hourly(false, Overlap::Queue);
        let fire = utc(2026, 11, 3, 4, 0);
        assert_eq!(s.decide(fire, plus(fire, -1), 0), Decision::Wait);
    }

    #[test]
    fn a_due_fire_starts_on_time() {
        let s = hourly(false, Overlap::Queue);
        let fire = utc(2026, 11, 3, 4, 0);
        assert_eq!(
            s.decide(fire, plus(fire, 500), 0),
            Decision::Start {
                logical_time: fire,
                trigger: Trigger::Cron,
                next_fire: Some(utc(2026, 11, 3, 5, 0)),
                cancel_active: false,
            }
        );
    }

    #[test]
    fn a_late_fire_is_a_catchup() {
        let s = hourly(false, Overlap::Queue);
        let fire = utc(2026, 11, 3, 4, 0);
        let Decision::Start { trigger, .. } = s.decide(fire, plus(fire, 2 * MINUTE), 0) else {
            panic!("expected a start");
        };
        assert_eq!(trigger, Trigger::Catchup);
    }

    #[test]
    fn without_catchup_only_the_latest_missed_fire_starts() {
        let s = hourly(false, Overlap::Queue);
        assert_eq!(
            s.decide(utc(2026, 11, 3, 0, 0), utc(2026, 11, 3, 3, 30), 0),
            Decision::Start {
                logical_time: utc(2026, 11, 3, 3, 0),
                trigger: Trigger::Catchup,
                next_fire: Some(utc(2026, 11, 3, 4, 0)),
                cancel_active: false,
            }
        );
    }

    #[test]
    fn with_catchup_missed_fires_start_one_at_a_time() {
        let s = hourly(true, Overlap::Queue);
        assert_eq!(
            s.decide(utc(2026, 11, 3, 0, 0), utc(2026, 11, 3, 3, 30), 0),
            Decision::Start {
                logical_time: utc(2026, 11, 3, 0, 0),
                trigger: Trigger::Catchup,
                // Still in the past: the next call starts the 01:00 run.
                next_fire: Some(utc(2026, 11, 3, 1, 0)),
                cancel_active: false,
            }
        );
    }

    #[test]
    fn queue_waits_for_active_runs() {
        let s = hourly(false, Overlap::Queue);
        let fire = utc(2026, 11, 3, 4, 0);
        assert_eq!(s.decide(fire, fire, 1), Decision::Wait);
    }

    #[test]
    fn skip_records_the_fire_and_moves_on() {
        let s = hourly(false, Overlap::Skip);
        let fire = utc(2026, 11, 3, 4, 0);
        assert_eq!(
            s.decide(fire, fire, 1),
            Decision::Skip {
                logical_time: fire,
                next_fire: Some(utc(2026, 11, 3, 5, 0)),
            }
        );
    }

    #[test]
    fn allow_starts_up_to_max_active_then_waits() {
        let s = hourly(false, Overlap::Allow { max_active: 2 });
        let fire = utc(2026, 11, 3, 4, 0);
        assert!(matches!(s.decide(fire, fire, 1), Decision::Start { .. }));
        assert_eq!(s.decide(fire, fire, 2), Decision::Wait);
    }

    #[test]
    fn replace_cancels_the_active_runs() {
        let s = hourly(false, Overlap::Replace);
        let fire = utc(2026, 11, 3, 4, 0);
        let Decision::Start { cancel_active, .. } = s.decide(fire, fire, 1) else {
            panic!("expected a start");
        };
        assert!(cancel_active);
    }

    #[test]
    fn rules_only_matter_while_runs_are_active() {
        let fire = utc(2026, 11, 3, 4, 0);
        for overlap in [Overlap::Queue, Overlap::Skip, Overlap::Replace] {
            let Decision::Start { cancel_active, .. } =
                hourly(false, overlap).decide(fire, fire, 0)
            else {
                panic!("{overlap:?} should start with no active run");
            };
            assert!(!cancel_active, "{overlap:?}");
        }
    }
}
