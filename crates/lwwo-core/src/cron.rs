//! Cron expressions, and the next minute they match, in local time.
//!
//! An expression has five fields: minute, hour, day of month, month and
//! day of week. Each field becomes a bit mask of the values it allows:
//!
//! ```text
//! "*/15 3 * * 1-5"
//! minute       {0, 15, 30, 45}   bits 0, 15, 30, 45
//! hour         {3}               bit 3
//! day          1..=31            every day
//! month        1..=12            every month
//! day of week  {1, 2, 3, 4, 5}   Monday to Friday
//! ```
//!
//! A field is a comma-separated list of `*`, `n` or `a-b`, each optionally
//! followed by a step `/s`; `n/s` means from `n` to the end of the field.
//! In the day of week, both 0 and 7 are Sunday.
//!
//! As in every common cron, when both day fields are restricted a day
//! matches if *either* does: `0 0 13 * 5` fires on the 13th and on every
//! Friday. When one of them is `*`, only the other one counts.

use std::fmt;
use std::str::FromStr;

use crate::calendar::{Date, DateTime};

/// How many days `next_after` looks ahead. Eight years plus a day cover
/// the longest gap between two February 29ths (2096 to 2104), so an
/// expression that finds nothing in this window never matches.
const SEARCH_DAYS: u32 = 8 * 366 + 1;

/// One of the five fields of a cron expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    /// Minute, 0 to 59.
    Minute,
    /// Hour, 0 to 23.
    Hour,
    /// Day of month, 1 to 31.
    DayOfMonth,
    /// Month, 1 to 12.
    Month,
    /// Day of week, 0 to 7, where both 0 and 7 are Sunday.
    DayOfWeek,
}

impl Field {
    /// The five fields, in the order they are written.
    const ALL: [Self; 5] = [
        Self::Minute,
        Self::Hour,
        Self::DayOfMonth,
        Self::Month,
        Self::DayOfWeek,
    ];

    /// The smallest and largest value the field accepts.
    const fn range(self) -> (u32, u32) {
        match self {
            Self::Minute => (0, 59),
            Self::Hour => (0, 23),
            Self::DayOfMonth => (1, 31),
            Self::Month => (1, 12),
            Self::DayOfWeek => (0, 7),
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::DayOfMonth => "day of month",
            Self::Month => "month",
            Self::DayOfWeek => "day of week",
        })
    }
}

/// Why a cron expression was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronError {
    /// The expression does not have exactly five fields.
    FieldCount(usize),
    /// A part of a field is not `*`, `n` or `a-b` with an optional `/s`,
    /// has a range that goes backwards, or has a step of zero.
    Invalid {
        /// The field.
        field: Field,
        /// The part that could not be read.
        part: String,
    },
    /// A number is outside the range of its field.
    OutOfRange {
        /// The field.
        field: Field,
        /// The number.
        value: u32,
    },
}

impl fmt::Display for CronError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldCount(count) => {
                write!(f, "a cron expression has 5 fields, found {count}")
            }
            Self::Invalid { field, part } => write!(f, "invalid {field}: `{part}`"),
            Self::OutOfRange { field, value } => {
                let (min, max) = field.range();
                write!(f, "{field} {value} is not between {min} and {max}")
            }
        }
    }
}

impl std::error::Error for CronError {}

/// A parsed cron expression.
///
/// Build it with [`str::parse`]: `"0 3 * * *".parse::<Cron>()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cron {
    /// Bit `n` is set when value `n` is allowed. Days of week use bits 0
    /// to 6: a 7 in the expression sets bit 0.
    minutes: u64,
    hours: u64,
    days: u64,
    months: u64,
    weekdays: u64,
    /// Both day fields are restricted, so a day matches if either does.
    either_day: bool,
}

impl FromStr for Cron {
    type Err = CronError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = text.split_whitespace().collect();
        if parts.len() != Field::ALL.len() {
            return Err(CronError::FieldCount(parts.len()));
        }

        let mut masks = [0; 5];
        for ((mask, field), part) in masks.iter_mut().zip(Field::ALL).zip(&parts) {
            *mask = parse_field(part, field)?;
        }
        let [minutes, hours, days, months, mut weekdays] = masks;

        // Sunday is both 0 and 7: keep it in bit 0 only.
        if weekdays & 1 << 7 != 0 {
            weekdays = (weekdays & !(1 << 7)) | 1;
        }

        Ok(Self {
            minutes,
            hours,
            days,
            months,
            weekdays,
            either_day: !parts[2].starts_with('*') && !parts[4].starts_with('*'),
        })
    }
}

impl Cron {
    /// The first minute strictly after `after` that the expression
    /// matches, or `None` if it matches no day in the next eight years,
    /// like `0 0 30 2 *` (February 30th).
    #[must_use]
    pub fn next_after(&self, after: DateTime) -> Option<DateTime> {
        let mut date = after.date();
        // Where the search starts on the current day; minute 60 simply
        // finds nothing in this hour and moves on to the next one.
        let mut from_hour = after.hour();
        let mut from_minute = after.minute() + 1;

        for _ in 0..SEARCH_DAYS {
            if self.matches_date(date) {
                while let Some(hour) = first_at_or_after(self.hours, from_hour) {
                    if hour != from_hour {
                        from_minute = 0;
                    }
                    if let Some(minute) = first_at_or_after(self.minutes, from_minute) {
                        return DateTime::new(date, hour, minute);
                    }
                    from_hour = hour + 1;
                    from_minute = 0;
                }
            }
            date = date.next_day();
            from_hour = 0;
            from_minute = 0;
        }
        None
    }

    /// Whether the expression allows `date`, by month and by day.
    fn matches_date(&self, date: Date) -> bool {
        if !has(self.months, date.month()) {
            return false;
        }
        let by_day = has(self.days, date.day());
        let by_weekday = has(self.weekdays, date.weekday());
        if self.either_day {
            by_day || by_weekday
        } else {
            by_day && by_weekday
        }
    }
}

/// Reads one field into a mask with bit `n` set for each allowed value.
///
/// ```text
/// "1,15,30"  bits 1, 15, 30
/// "10-20/5"  bits 10, 15, 20
/// "5/20"     (minute) bits 5, 25, 45
/// "*/0"      Invalid: a step must be at least 1
/// "60"       (minute) OutOfRange
/// ```
fn parse_field(text: &str, field: Field) -> Result<u64, CronError> {
    let (min, max) = field.range();
    let mut mask = 0;

    for part in text.split(',') {
        let invalid = || CronError::Invalid {
            field,
            part: part.to_owned(),
        };
        let number = |digits: &str| -> Result<u32, CronError> {
            let value: u32 = digits.parse().map_err(|_| invalid())?;
            if value < min || value > max {
                return Err(CronError::OutOfRange { field, value });
            }
            Ok(value)
        };

        let (range, step) = match part.split_once('/') {
            Some((range, step)) => (range, Some(step.parse::<u32>().map_err(|_| invalid())?)),
            None => (part, None),
        };
        let (start, end) = if range == "*" {
            (min, max)
        } else if let Some((start, end)) = range.split_once('-') {
            (number(start)?, number(end)?)
        } else {
            // `n/s` runs from n to the end of the field; a lone `n` is n.
            let start = number(range)?;
            (start, if step.is_some() { max } else { start })
        };
        let step = step.unwrap_or(1);
        if start > end || step == 0 {
            return Err(invalid());
        }

        let mut value = start;
        while value <= end {
            mask |= 1 << value;
            value += step;
        }
    }
    Ok(mask)
}

/// Whether bit `value` is set in `mask`.
fn has(mask: u64, value: u8) -> bool {
    mask & 1 << value != 0
}

/// The smallest value at or after `from` whose bit is set in `mask`, or
/// `None` if there is none.
///
/// ```text
/// mask {0, 15, 30, 45}, from 20  →  30
/// mask {0, 15, 30, 45}, from 46  →  None
/// ```
fn first_at_or_after(mask: u64, from: u8) -> Option<u8> {
    if from >= 64 {
        return None;
    }
    // Clear the bits below `from`, then count the zeros up to the first one.
    let rest = mask & (u64::MAX << from);
    if rest == 0 {
        return None;
    }
    // trailing_zeros is below 64 here, so it fits in a u8.
    u8::try_from(rest.trailing_zeros()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mask with the given bits set.
    fn bits(values: &[u32]) -> u64 {
        values.iter().fold(0, |mask, v| mask | 1 << v)
    }

    fn cron(text: &str) -> Cron {
        text.parse().expect("valid cron in test")
    }

    fn at(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> DateTime {
        let date = Date::new(year, month, day).expect("valid date in test");
        DateTime::new(date, hour, minute).expect("valid time in test")
    }

    #[test]
    fn expressions_need_five_fields() {
        assert_eq!("0 3 * *".parse::<Cron>(), Err(CronError::FieldCount(4)));
        assert_eq!("0 3 * * * *".parse::<Cron>(), Err(CronError::FieldCount(6)));
    }

    #[test]
    fn first_at_or_after_finds_the_next_bit() {
        let quarters = bits(&[0, 15, 30, 45]);
        assert_eq!(first_at_or_after(quarters, 0), Some(0));
        assert_eq!(first_at_or_after(quarters, 15), Some(15));
        assert_eq!(first_at_or_after(quarters, 20), Some(30));
        assert_eq!(first_at_or_after(quarters, 46), None);
        assert_eq!(first_at_or_after(quarters, 64), None);
    }

    #[test]
    fn parses_stars_numbers_and_ranges() {
        let c = cron("*/15 3 * * 1-5");
        assert_eq!(c.minutes, bits(&[0, 15, 30, 45]));
        assert_eq!(c.hours, bits(&[3]));
        assert_eq!(c.days, bits(&(1..=31).collect::<Vec<_>>()));
        assert_eq!(c.months, bits(&(1..=12).collect::<Vec<_>>()));
        assert_eq!(c.weekdays, bits(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn parses_lists_and_steps() {
        assert_eq!(
            parse_field("1,15,30", Field::Minute),
            Ok(bits(&[1, 15, 30]))
        );
        assert_eq!(
            parse_field("10-20/5", Field::Minute),
            Ok(bits(&[10, 15, 20]))
        );
        assert_eq!(parse_field("5/20", Field::Minute), Ok(bits(&[5, 25, 45])));
        assert_eq!(parse_field("1-3,20", Field::Hour), Ok(bits(&[1, 2, 3, 20])));
    }

    #[test]
    fn seven_is_sunday() {
        assert_eq!(cron("0 0 * * 7").weekdays, bits(&[0]));
        assert_eq!(cron("0 0 * * 5-7").weekdays, bits(&[0, 5, 6]));
    }

    #[test]
    fn rejects_numbers_out_of_range() {
        assert_eq!(
            "60 * * * *".parse::<Cron>(),
            Err(CronError::OutOfRange {
                field: Field::Minute,
                value: 60
            })
        );
        assert_eq!(
            "0 0 0 * *".parse::<Cron>(),
            Err(CronError::OutOfRange {
                field: Field::DayOfMonth,
                value: 0
            })
        );
    }

    #[test]
    fn rejects_malformed_parts() {
        for (text, part) in [
            ("a", "a"),
            ("*/0", "*/0"),
            ("5-1", "5-1"),
            ("1,,2", ""),
            ("1-", "1-"),
            ("*/", "*/"),
        ] {
            assert_eq!(
                parse_field(text, Field::Minute),
                Err(CronError::Invalid {
                    field: Field::Minute,
                    part: part.to_owned()
                }),
                "{text}"
            );
        }
    }

    #[test]
    fn next_is_strictly_after() {
        let daily = cron("0 3 * * *");
        assert_eq!(
            daily.next_after(at(2026, 11, 3, 2, 59)),
            Some(at(2026, 11, 3, 3, 0))
        );
        assert_eq!(
            daily.next_after(at(2026, 11, 3, 3, 0)),
            Some(at(2026, 11, 4, 3, 0))
        );
    }

    #[test]
    fn next_moves_to_the_next_hour() {
        assert_eq!(
            cron("*/15 * * * *").next_after(at(2026, 11, 3, 10, 50)),
            Some(at(2026, 11, 3, 11, 0))
        );
    }

    #[test]
    fn next_skips_days_of_week() {
        // Friday 10:00 → Monday 09:00.
        assert_eq!(
            cron("0 9 * * 1-5").next_after(at(2026, 11, 6, 10, 0)),
            Some(at(2026, 11, 9, 9, 0))
        );
    }

    #[test]
    fn next_skips_short_months() {
        assert_eq!(
            cron("0 0 31 * *").next_after(at(2026, 11, 1, 0, 0)),
            Some(at(2026, 12, 31, 0, 0))
        );
    }

    #[test]
    fn next_rolls_over_the_year() {
        assert_eq!(
            cron("* * * * *").next_after(at(2026, 12, 31, 23, 59)),
            Some(at(2027, 1, 1, 0, 0))
        );
    }

    #[test]
    fn next_finds_february_29th() {
        assert_eq!(
            cron("0 0 29 2 *").next_after(at(2026, 3, 1, 0, 0)),
            Some(at(2028, 2, 29, 0, 0))
        );
    }

    #[test]
    fn next_takes_either_day_when_both_are_restricted() {
        // The 13th or a Friday: Friday 2026-11-06 comes first.
        assert_eq!(
            cron("0 0 13 * 5").next_after(at(2026, 11, 3, 0, 0)),
            Some(at(2026, 11, 6, 0, 0))
        );
    }

    #[test]
    fn impossible_dates_never_match() {
        assert_eq!(cron("0 0 30 2 *").next_after(at(2026, 1, 1, 0, 0)), None);
    }
}
