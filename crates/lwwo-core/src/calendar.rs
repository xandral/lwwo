//! Civil dates and times: what a wall clock shows, with no time zone.
//!
//! Cron expressions are read in the time zone of each DAG, so the next fire
//! is first found here, in local time, and only then turned into a UTC
//! instant. The calendar is the proleptic Gregorian one: leap years every
//! four years, except centuries not divisible by 400.

/// A day in the civil calendar.
///
/// Dates are ordered by year, then month, then day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    year: i32,
    month: u8,
    day: u8,
}

impl Date {
    /// The date `year-month-day`, or `None` if that day does not exist,
    /// like February 30th.
    #[must_use]
    pub fn new(year: i32, month: u8, day: u8) -> Option<Self> {
        if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
            return None;
        }
        Some(Self { year, month, day })
    }

    /// The year.
    #[must_use]
    pub const fn year(self) -> i32 {
        self.year
    }

    /// The month, from 1 (January) to 12.
    #[must_use]
    pub const fn month(self) -> u8 {
        self.month
    }

    /// The day of the month, from 1.
    #[must_use]
    pub const fn day(self) -> u8 {
        self.day
    }

    /// The following day.
    #[must_use]
    pub fn next_day(self) -> Self {
        if self.day < days_in_month(self.year, self.month) {
            Self {
                day: self.day + 1,
                ..self
            }
        } else if self.month < 12 {
            Self {
                month: self.month + 1,
                day: 1,
                ..self
            }
        } else {
            Self {
                year: self.year + 1,
                month: 1,
                day: 1,
            }
        }
    }

    /// Days since 1970-01-01, negative before it.
    ///
    /// This is `days_from_civil` by Howard Hinnant: it counts years from
    /// March, so that the leap day falls at the end of the year, and works
    /// in 400-year eras, which all have the same number of days.
    #[must_use]
    pub fn days_since_epoch(self) -> i64 {
        let month = i64::from(self.month);
        let day = i64::from(self.day);
        let year = i64::from(self.year) - i64::from(month <= 2);

        let era = year.div_euclid(400);
        let year_of_era = year - era * 400; // 0..=399
        let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1; // 0..=365
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

        era * 146_097 + day_of_era - 719_468
    }

    /// The date `days` days after 1970-01-01 (before it, if negative): the
    /// inverse of [`days_since_epoch`](Self::days_since_epoch).
    ///
    /// This is `civil_from_days` by Howard Hinnant. Any day reachable from
    /// a millisecond `i64` timestamp has a year that fits in an `i32`.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "month and day are small, and the year fits as documented"
    )]
    pub fn from_days_since_epoch(days: i64) -> Self {
        let days = days + 719_468;
        let era = days.div_euclid(146_097);
        let day_of_era = days - era * 146_097; // 0..=146_096
        let year_of_era =
            (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_from_march = (5 * day_of_year + 2) / 153; // 0 is March
        let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
        let month = if month_from_march < 10 {
            month_from_march + 3
        } else {
            month_from_march - 9
        };
        let year = year_of_era + era * 400 + i64::from(month <= 2);

        Self {
            year: year as i32,
            month: month as u8,
            day: day as u8,
        }
    }

    /// The day of the week, from 0 (Sunday) to 6 (Saturday), as in cron.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "rem_euclid(7) is always in 0..7"
    )]
    pub fn weekday(self) -> u8 {
        // 1970-01-01 was a Thursday, and rem_euclid stays positive before it.
        (self.days_since_epoch() + 4).rem_euclid(7) as u8
    }
}

/// A minute on a wall clock: a date, an hour and a minute.
///
/// Cron works to the minute, so there are no seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime {
    date: Date,
    hour: u8,
    minute: u8,
}

impl DateTime {
    /// `hour:minute` on `date`, or `None` if the hour is not in `0..24` or
    /// the minute is not in `0..60`.
    #[must_use]
    pub fn new(date: Date, hour: u8, minute: u8) -> Option<Self> {
        if hour >= 24 || minute >= 60 {
            return None;
        }
        Some(Self { date, hour, minute })
    }

    /// The date.
    #[must_use]
    pub const fn date(self) -> Date {
        self.date
    }

    /// The hour, from 0 to 23.
    #[must_use]
    pub const fn hour(self) -> u8 {
        self.hour
    }

    /// The minute, from 0 to 59.
    #[must_use]
    pub const fn minute(self) -> u8 {
        self.minute
    }
}

/// Whether `year` has a February 29th.
#[must_use]
pub const fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// The number of days in `month` (1 to 12) of `year`.
///
/// # Panics
///
/// Panics if `month` is not in `1..=12`.
#[must_use]
pub const fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => panic!("month must be between 1 and 12"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u8, day: u8) -> Date {
        Date::new(year, month, day).expect("valid date in test")
    }

    // Already working

    #[test]
    fn days_since_epoch_counts_from_1970() {
        assert_eq!(date(1970, 1, 1).days_since_epoch(), 0);
        assert_eq!(date(1970, 1, 2).days_since_epoch(), 1);
        assert_eq!(date(2000, 3, 1).days_since_epoch(), 11_017);
    }

    #[test]
    fn days_since_epoch_is_negative_before_1970() {
        assert_eq!(date(1969, 12, 31).days_since_epoch(), -1);
    }

    #[test]
    fn from_days_since_epoch_inverts_days_since_epoch() {
        for days in -800_000..800_000 {
            let date = Date::from_days_since_epoch(days);
            assert_eq!(date.days_since_epoch(), days, "{date:?}");
        }
        assert_eq!(Date::from_days_since_epoch(0), date(1970, 1, 1));
        assert_eq!(Date::from_days_since_epoch(11_017), date(2000, 3, 1));
        assert_eq!(Date::from_days_since_epoch(-1), date(1969, 12, 31));
    }

    #[test]
    fn date_times_reject_impossible_hours_and_minutes() {
        let day = date(2026, 11, 3);
        assert!(DateTime::new(day, 23, 59).is_some());
        assert!(DateTime::new(day, 24, 0).is_none());
        assert!(DateTime::new(day, 0, 60).is_none());
    }

    // TASK 14

    #[test]
    fn leap_years() {
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(2026));
        assert!(!is_leap_year(1900));
        assert!(is_leap_year(2000));
    }

    // TASK 15 (needs 14)

    #[test]
    fn days_in_each_month() {
        assert_eq!(days_in_month(2026, 1), 31);
        assert_eq!(days_in_month(2026, 4), 30);
        assert_eq!(days_in_month(2026, 12), 31);
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2026, 2), 28);
        assert_eq!(days_in_month(1900, 2), 28);
    }

    #[test]
    #[should_panic(expected = "month")]
    fn days_in_month_rejects_month_13() {
        let _ = days_in_month(2026, 13);
    }

    #[test]
    fn dates_that_do_not_exist_are_rejected() {
        assert!(Date::new(2024, 2, 29).is_some());
        assert!(Date::new(2026, 2, 29).is_none());
        assert!(Date::new(2026, 4, 31).is_none());
        assert!(Date::new(2026, 13, 1).is_none());
        assert!(Date::new(2026, 1, 0).is_none());
    }

    #[test]
    fn next_day_rolls_over_months_and_years() {
        assert_eq!(date(2026, 11, 3).next_day(), date(2026, 11, 4));
        assert_eq!(date(2026, 4, 30).next_day(), date(2026, 5, 1));
        assert_eq!(date(2024, 2, 28).next_day(), date(2024, 2, 29));
        assert_eq!(date(2026, 2, 28).next_day(), date(2026, 3, 1));
        assert_eq!(date(2026, 12, 31).next_day(), date(2027, 1, 1));
    }

    // TASK 16

    #[test]
    fn weekdays_start_from_sunday() {
        assert_eq!(date(1970, 1, 1).weekday(), 4); // Thursday
        assert_eq!(date(2000, 1, 1).weekday(), 6); // Saturday
        assert_eq!(date(2026, 11, 3).weekday(), 2); // Tuesday
    }

    #[test]
    fn weekdays_before_1970() {
        assert_eq!(date(1969, 12, 28).weekday(), 0); // Sunday
    }
}
