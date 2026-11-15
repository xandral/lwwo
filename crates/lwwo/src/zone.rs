//! Time zones for the core, from the IANA database.
//!
//! The core computes cron fires in wall-clock time and asks a
//! [`TimeZone`] to go to UTC and back. Here that is done by jiff, which
//! reads `/usr/share/zoneinfo` and falls back to a copy inside the binary.

use anyhow::{Context, Result};
use jiff::tz::AmbiguousOffset;
use lwwo_core::calendar::{Date, DateTime};
use lwwo_core::time::{LocalTime, TimeZone, Timestamp};

/// An IANA time zone, such as `Europe/Rome`.
#[derive(Debug, Clone)]
pub(crate) struct Zone(jiff::tz::TimeZone);

impl Zone {
    /// The zone with this IANA name.
    pub(crate) fn named(name: &str) -> Result<Self> {
        jiff::tz::TimeZone::get(name)
            .map(Self)
            .with_context(|| format!("unknown time zone `{name}`"))
    }

    /// The jiff zone, for formatting.
    pub(crate) fn inner(&self) -> &jiff::tz::TimeZone {
        &self.0
    }

    /// `instant` as `2026-10-26T03:00:00+01:00` in this zone.
    pub(crate) fn rfc3339(&self, instant: i64) -> String {
        to_jiff(instant)
            .to_zoned(self.0.clone())
            .strftime("%Y-%m-%dT%H:%M:%S%:z")
            .to_string()
    }

    /// The day of `instant` in this zone, as `2026-10-26`.
    pub(crate) fn date(&self, instant: i64) -> String {
        to_jiff(instant)
            .to_zoned(self.0.clone())
            .strftime("%Y-%m-%d")
            .to_string()
    }
}

fn to_jiff(millis: i64) -> jiff::Timestamp {
    jiff::Timestamp::from_millisecond(millis).unwrap_or(if millis < 0 {
        jiff::Timestamp::MIN
    } else {
        jiff::Timestamp::MAX
    })
}

/// The instant at which the wall clock, `offset` seconds ahead of UTC,
/// shows `local`.
fn at_offset(local: DateTime, offset: jiff::tz::Offset) -> Timestamp {
    Timestamp::from_local(local, i64::from(offset.seconds()) * 1_000)
}

impl TimeZone for Zone {
    fn local(&self, instant: Timestamp) -> DateTime {
        let civil = self.0.to_datetime(to_jiff(instant.as_millis()));
        let date = Date::new(
            i32::from(civil.year()),
            civil.month().unsigned_abs(),
            civil.day().unsigned_abs(),
        )
        .expect("jiff dates are valid");
        DateTime::new(
            date,
            civil.hour().unsigned_abs(),
            civil.minute().unsigned_abs(),
        )
        .expect("jiff times are valid")
    }

    fn resolve(&self, local: DateTime) -> LocalTime {
        let date = local.date();
        let civil = jiff::civil::DateTime::new(
            i16::try_from(date.year()).unwrap_or(i16::MAX),
            i8::try_from(date.month()).unwrap_or(1),
            i8::try_from(date.day()).unwrap_or(1),
            i8::try_from(local.hour()).unwrap_or(0),
            i8::try_from(local.minute()).unwrap_or(0),
            0,
            0,
        );
        let Ok(civil) = civil else {
            // A year jiff cannot represent: read it as UTC.
            return LocalTime::Unique(Timestamp::from_local(local, 0));
        };
        match self.0.to_ambiguous_zoned(civil).offset() {
            AmbiguousOffset::Unambiguous { offset } => LocalTime::Unique(at_offset(local, offset)),
            AmbiguousOffset::Fold { before, after } => LocalTime::Repeated {
                first: at_offset(local, before),
                second: at_offset(local, after),
            },
            AmbiguousOffset::Gap { before, .. } => {
                // Read with the offset from before the jump, the missing
                // time lands after it; the jump itself is the last
                // transition before that instant.
                let after_jump = at_offset(local, before);
                let probe = to_jiff(after_jump.as_millis().saturating_add(1));
                let resume = self.0.preceding(probe).next().map_or(after_jump, |jump| {
                    Timestamp::from_millis(jump.timestamp().as_millisecond())
                });
                LocalTime::Skipped { resume }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> DateTime {
        DateTime::new(Date::new(year, month, day).unwrap(), hour, minute).unwrap()
    }

    fn utc(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> Timestamp {
        Timestamp::from_local(local(year, month, day, hour, minute), 0)
    }

    #[test]
    fn rome_is_one_hour_ahead_in_winter_and_two_in_summer() {
        let rome = Zone::named("Europe/Rome").unwrap();
        assert_eq!(
            rome.local(utc(2026, 1, 15, 12, 0)),
            local(2026, 1, 15, 13, 0)
        );
        assert_eq!(
            rome.local(utc(2026, 7, 15, 12, 0)),
            local(2026, 7, 15, 14, 0)
        );
        assert_eq!(
            rome.resolve(local(2026, 7, 15, 14, 0)),
            LocalTime::Unique(utc(2026, 7, 15, 12, 0))
        );
    }

    #[test]
    fn a_time_the_clocks_jump_over_resumes_at_the_jump() {
        // 29 March 2026: at 02:00 the clocks in Rome go to 03:00.
        let rome = Zone::named("Europe/Rome").unwrap();
        let jump = utc(2026, 3, 29, 1, 0);
        assert_eq!(
            rome.resolve(local(2026, 3, 29, 2, 30)),
            LocalTime::Skipped { resume: jump }
        );
        assert_eq!(
            rome.resolve(local(2026, 3, 29, 2, 0)),
            LocalTime::Skipped { resume: jump }
        );
        assert_eq!(rome.local(jump), local(2026, 3, 29, 3, 0));
    }

    #[test]
    fn a_time_that_happens_twice_gives_both_instants() {
        // 25 October 2026: at 03:00 the clocks in Rome go back to 02:00.
        let rome = Zone::named("Europe/Rome").unwrap();
        assert_eq!(
            rome.resolve(local(2026, 10, 25, 2, 30)),
            LocalTime::Repeated {
                first: utc(2026, 10, 25, 0, 30),
                second: utc(2026, 10, 25, 1, 30),
            }
        );
    }

    #[test]
    fn formats_follow_the_zone() {
        let rome = Zone::named("Europe/Rome").unwrap();
        let instant = utc(2026, 10, 26, 2, 0).as_millis();
        assert_eq!(rome.rfc3339(instant), "2026-10-26T03:00:00+01:00");
        assert_eq!(
            rome.date(utc(2026, 10, 25, 23, 30).as_millis()),
            "2026-10-26"
        );
        assert!(Zone::named("Mars/Olympus").is_err());
    }
}
