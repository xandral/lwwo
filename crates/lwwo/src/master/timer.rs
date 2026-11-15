//! The one alarm of the master.
//!
//! The master computes when the next thing is due, a cron fire, a retry or
//! a heartbeat expiry, and sleeps until then. On Linux the alarm is a
//! `timerfd` armed on an absolute instant with `TFD_TIMER_CANCEL_ON_SET`:
//! the kernel also wakes the master when someone moves the system clock.
//! Elsewhere there is no such thing, and the master looks at the clock at
//! least once a minute instead.

use std::io;

/// Why [`Alarm::wait`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ring {
    /// The instant the alarm was set on may have come: check what is due.
    Due,
    /// The system clock was moved: everything must be computed again.
    ClockChanged,
}

#[cfg(target_os = "linux")]
mod imp {
    use std::io;
    use std::os::fd::OwnedFd;

    use rustix::io::Errno;
    use rustix::time::{
        Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec, timerfd_create,
        timerfd_settime,
    };
    use tokio::io::Interest;
    use tokio::io::unix::AsyncFd;

    use super::Ring;

    #[derive(Debug)]
    pub(crate) struct Alarm {
        fd: AsyncFd<OwnedFd>,
        /// What the timer is armed on, to skip system calls that would
        /// change nothing. `None` also after a clock change, which disarms.
        armed: Option<i64>,
    }

    impl Alarm {
        // tokio deprecated this constructor in favour of an `unsafe` one,
        // because with a borrowed descriptor the caller could close it
        // while tokio still watches it. Here the `AsyncFd` owns the
        // descriptor for its whole life, which is exactly what the unsafe
        // one asks to promise; and this workspace forbids `unsafe`.
        #[allow(deprecated, reason = "the AsyncFd owns the descriptor; see above")]
        pub(crate) fn new() -> io::Result<Self> {
            let fd = timerfd_create(
                TimerfdClockId::Realtime,
                TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC,
            )?;
            Ok(Self {
                fd: AsyncFd::with_interest(fd, Interest::READABLE)?,
                armed: None,
            })
        }

        pub(crate) fn arm(&mut self, at: Option<i64>) -> io::Result<()> {
            if at == self.armed {
                return Ok(());
            }
            let zero = Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // An instant in the past fires at once; all zeros would disarm.
            let value = at.map_or(zero, |millis| {
                let millis = millis.max(1);
                Timespec {
                    tv_sec: millis / 1_000,
                    tv_nsec: (millis % 1_000) * 1_000_000,
                }
            });
            // ABSTIME: an instant, not a duration. CANCEL_ON_SET: wake me
            // if the clock is moved. Disarmed, nothing is due, so a moved
            // clock changes nothing: no flags, or the kernel would still
            // wake us for it.
            let flags = if at.is_some() {
                TimerfdTimerFlags::ABSTIME | TimerfdTimerFlags::CANCEL_ON_SET
            } else {
                TimerfdTimerFlags::empty()
            };
            timerfd_settime(
                self.fd.get_ref(),
                flags,
                &Itimerspec {
                    it_interval: zero,
                    it_value: value,
                },
            )?;
            self.armed = at;
            Ok(())
        }

        pub(crate) async fn wait(&mut self) -> io::Result<Ring> {
            loop {
                let mut guard = self.fd.readable().await?;
                let mut buffer = [0u8; 8];
                let read = guard
                    .try_io(|fd| rustix::io::read(fd.get_ref(), &mut buffer).map_err(Into::into));
                match read {
                    Ok(Ok(_)) => {
                        self.armed = None;
                        return Ok(Ring::Due);
                    }
                    Ok(Err(error))
                        if error.raw_os_error() == Some(Errno::CANCELED.raw_os_error()) =>
                    {
                        // The kernel disarmed the timer: arm it again.
                        self.armed = None;
                        return Ok(Ring::ClockChanged);
                    }
                    Ok(Err(error)) => return Err(error),
                    // Woken for nothing: back to sleep.
                    Err(_would_block) => {}
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;
    use std::time::Duration;

    use super::Ring;
    use crate::util::now_ms;

    /// Without `timerfd` nobody says when the clock moves, so never sleep
    /// longer than this before looking at it again.
    const LONGEST_SLEEP: Duration = Duration::from_secs(60);

    #[derive(Debug)]
    pub(crate) struct Alarm {
        at: Option<i64>,
    }

    impl Alarm {
        #[expect(clippy::unnecessary_wraps, reason = "same signature as on Linux")]
        pub(crate) fn new() -> io::Result<Self> {
            Ok(Self { at: None })
        }

        #[expect(clippy::unnecessary_wraps, reason = "same signature as on Linux")]
        pub(crate) fn arm(&mut self, at: Option<i64>) -> io::Result<()> {
            self.at = at;
            Ok(())
        }

        pub(crate) async fn wait(&mut self) -> io::Result<Ring> {
            let Some(at) = self.at else {
                // Nothing is due, so a moved clock changes nothing either.
                return std::future::pending().await;
            };
            let left = u64::try_from(at - now_ms()).unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(left).min(LONGEST_SLEEP)).await;
            Ok(Ring::Due)
        }
    }
}

/// The alarm of the master: armed on one instant, it wakes whoever waits.
#[derive(Debug)]
pub(crate) struct Alarm(imp::Alarm);

impl Alarm {
    /// An alarm that is not armed.
    pub(crate) fn new() -> io::Result<Self> {
        imp::Alarm::new().map(Self)
    }

    /// Arms the alarm on `at`, UTC milliseconds, or disarms it.
    pub(crate) fn arm(&mut self, at: Option<i64>) -> io::Result<()> {
        self.0.arm(at)
    }

    /// Sleeps until the alarm rings. With the alarm disarmed it never
    /// returns, which is what a `select!` wants.
    pub(crate) async fn wait(&mut self) -> io::Result<Ring> {
        self.0.wait().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::now_ms;

    #[tokio::test]
    async fn rings_at_the_instant_it_was_armed_on() {
        let mut alarm = Alarm::new().unwrap();
        let start = now_ms();
        alarm.arm(Some(start + 150)).unwrap();
        assert_eq!(alarm.wait().await.unwrap(), Ring::Due);
        let waited = now_ms() - start;
        assert!((140..2_000).contains(&waited), "waited {waited} ms");
    }

    #[tokio::test]
    async fn an_instant_in_the_past_rings_at_once() {
        let mut alarm = Alarm::new().unwrap();
        alarm.arm(Some(now_ms() - 5_000)).unwrap();
        let ring = tokio::time::timeout(std::time::Duration::from_secs(2), alarm.wait()).await;
        assert_eq!(ring.unwrap().unwrap(), Ring::Due);
    }

    #[tokio::test]
    async fn disarmed_it_never_rings() {
        let mut alarm = Alarm::new().unwrap();
        alarm.arm(Some(now_ms() + 50)).unwrap();
        alarm.arm(None).unwrap();
        let ring = tokio::time::timeout(std::time::Duration::from_millis(300), alarm.wait()).await;
        assert!(ring.is_err());
    }
}
