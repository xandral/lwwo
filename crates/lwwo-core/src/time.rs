//! Instants in UTC, and the time zones that turn them into wall-clock time.
//!
//! The core never reads the clock: every instant comes in as a
//! [`Timestamp`]. Time zones come in through the [`TimeZone`] trait, which
//! the binary implements with the IANA time zone database; the core only
//! ships [`Utc`].

use std::time::Duration;

use crate::calendar::{Date, DateTime};

const MS_PER_MINUTE: i64 = 60_000;
const MS_PER_DAY: i64 = 24 * 60 * MS_PER_MINUTE;

/// An instant, in milliseconds since 1970-01-01 00:00 UTC, as stored in
/// the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The instant `millis` milliseconds after 1970-01-01 00:00 UTC.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Milliseconds since 1970-01-01 00:00 UTC.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// The instant `duration` after this one, stopping at the largest
    /// instant instead of overflowing.
    #[must_use]
    pub fn saturating_add(self, duration: Duration) -> Self {
        let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        Self(self.0.saturating_add(millis))
    }

    /// The instant at which a clock `offset` milliseconds ahead of UTC
    /// shows `local`.
    #[must_use]
    pub fn from_local(local: DateTime, offset: i64) -> Self {
        let minutes = i64::from(local.hour()) * 60 + i64::from(local.minute());
        Self(local.date().days_since_epoch() * MS_PER_DAY + minutes * MS_PER_MINUTE - offset)
    }

    /// What a clock `offset` milliseconds ahead of UTC shows at this
    /// instant, to the minute.
    ///
    /// # Panics
    ///
    /// Never in practice: the hour and minute are computed from a minute of
    /// the day, so they are always valid.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "hour and minute are below 24 and 60"
    )]
    pub fn to_local(self, offset: i64) -> DateTime {
        let millis = self.0 + offset;
        let date = Date::from_days_since_epoch(millis.div_euclid(MS_PER_DAY));
        let minute_of_day = millis.rem_euclid(MS_PER_DAY) / MS_PER_MINUTE;
        DateTime::new(date, (minute_of_day / 60) as u8, (minute_of_day % 60) as u8)
            .expect("hour and minute come from a minute of the day")
    }
}

/// When a wall-clock time happens in a time zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTime {
    /// Once, as on most days.
    Unique(Timestamp),
    /// Twice, because the clocks went back over it, as at 02:30 on the
    /// last Sunday of October in Rome.
    Repeated {
        /// The first time, before the clocks went back.
        first: Timestamp,
        /// The second time.
        second: Timestamp,
    },
    /// Never, because the clocks jumped over it, as at 02:30 on the last
    /// Sunday of March in Rome.
    Skipped {
        /// The instant of the jump: the first valid time after it.
        resume: Timestamp,
    },
}

/// A time zone: the rules that turn UTC instants into wall-clock time and
/// back.
pub trait TimeZone {
    /// What the wall clock shows at `instant`, to the minute.
    fn local(&self, instant: Timestamp) -> DateTime;

    /// When the wall clock shows `local`.
    fn resolve(&self, local: DateTime) -> LocalTime;
}

/// Coordinated Universal Time: no offset and no daylight saving time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Utc;

impl TimeZone for Utc {
    fn local(&self, instant: Timestamp) -> DateTime {
        instant.to_local(0)
    }

    fn resolve(&self, local: DateTime) -> LocalTime {
        LocalTime::Unique(Timestamp::from_local(local, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 60 * MS_PER_MINUTE;

    fn at(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> DateTime {
        let date = Date::new(year, month, day).expect("valid date in test");
        DateTime::new(date, hour, minute).expect("valid time in test")
    }

    #[test]
    fn epoch_is_zero() {
        assert_eq!(
            Timestamp::from_local(at(1970, 1, 1, 0, 0), 0),
            Timestamp::from_millis(0)
        );
    }

    #[test]
    fn offsets_move_the_instant() {
        // 03:00 in Rome in winter (UTC+1) is 02:00 UTC.
        let rome = Timestamp::from_local(at(2026, 11, 3, 3, 0), HOUR);
        assert_eq!(rome, Timestamp::from_local(at(2026, 11, 3, 2, 0), 0));
        assert_eq!(rome.to_local(HOUR), at(2026, 11, 3, 3, 0));
    }

    #[test]
    fn to_local_drops_seconds_and_works_before_1970() {
        let instant = Timestamp::from_millis(-1);
        assert_eq!(instant.to_local(0), at(1969, 12, 31, 23, 59));
    }

    #[test]
    fn utc_round_trips() {
        let local = at(2026, 11, 3, 22, 15);
        let LocalTime::Unique(instant) = Utc.resolve(local) else {
            panic!("UTC times are unique");
        };
        assert_eq!(Utc.local(instant), local);
    }

    #[test]
    fn saturating_add_stops_at_the_end_of_time() {
        let t = Timestamp::from_millis(1_000);
        assert_eq!(
            t.saturating_add(Duration::from_secs(30)),
            Timestamp::from_millis(31_000)
        );
        assert_eq!(
            t.saturating_add(Duration::MAX),
            Timestamp::from_millis(i64::MAX)
        );
    }
}
