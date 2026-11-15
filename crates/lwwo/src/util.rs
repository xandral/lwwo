//! Small helpers shared by every part of the binary: the clock, and the
//! formats of durations, sizes and instants.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use lwwo_core::run::RunId;
use lwwo_core::time::Timestamp;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The current time as UTC milliseconds. The core never reads the clock:
/// this is where its `now` comes from.
pub(crate) fn now_ms() -> i64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)
}

/// The current time for the core.
pub(crate) fn now() -> Timestamp {
    Timestamp::from_millis(now_ms())
}

/// Parses a duration such as `30s`, `5m`, `1h` or `30d`.
pub(crate) fn parse_duration(text: &str) -> Result<Duration> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .with_context(|| format!("duration `{text}` has no unit: use s, m, h or d"))?;
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .with_context(|| format!("duration `{text}` does not start with a number"))?;
    let seconds = match unit.trim() {
        "ms" => return Ok(Duration::from_millis(number)),
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        other => bail!("unknown unit `{other}` in duration `{text}`: use s, m, h or d"),
    };
    Ok(Duration::from_secs(number.saturating_mul(seconds)))
}

/// A size: a number of bytes, or a share of the whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Size {
    /// So many bytes.
    Bytes(u64),
    /// So many hundredths of the total.
    Percent(u64),
}

impl Size {
    /// The size in bytes, given the total a percentage refers to.
    pub(crate) fn of(self, total: u64) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Percent(percent) => total / 100 * percent,
        }
    }
}

/// Parses a size such as `100MB`, `20GB` or `10%`.
pub(crate) fn parse_size(text: &str) -> Result<Size> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .with_context(|| format!("size `{text}` does not start with a number"))?;
    let factor: u64 = match unit.trim().to_ascii_uppercase().as_str() {
        "%" => return Ok(Size::Percent(number)),
        "" | "B" => 1,
        "KB" => 1 << 10,
        "MB" => 1 << 20,
        "GB" => 1 << 30,
        "TB" => 1 << 40,
        other => bail!("unknown unit `{other}` in size `{text}`: use KB, MB, GB, TB or %"),
    };
    Ok(Size::Bytes(number.saturating_mul(factor)))
}

/// Lowercase hexadecimal of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// Takes `dir` for this process alone, for as long as the returned file
/// stays open.
///
/// "One writer" is what the durability of the master, and the journal of
/// the worker, rest on. Two masters on the same database would each hand
/// out the same tasks; two workers on the same directory would each pick
/// up the same attempts. So a second one refuses to start. The lock is an
/// advisory one on a file (`flock`): it needs no cleaning up after a
/// crash, since the kernel releases it with the process, and it is only
/// reliable on a local disk, which is where these directories belong.
pub(crate) fn lock_dir(dir: &std::path::Path, who: &str) -> Result<std::fs::File> {
    let path = dir.join("lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {}
        Err(rustix::io::Errno::WOULDBLOCK) => bail!(
            "another lwwo {who} is already running on {}: two of them on the same data \
             would run tasks twice. Stop the other one, or give this one its own data directory",
            dir.display()
        ),
        Err(error) => {
            return Err(error).with_context(|| format!("cannot lock {}", path.display()));
        }
    }
    // Who holds it, for whoever finds the file.
    let _ = std::fs::write(&path, format!("{}\n", std::process::id()));
    Ok(file)
}

/// SHA-256 of `bytes`, in hexadecimal.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// A new run id: a `UUIDv7`, so that ids sort by creation time.
pub(crate) fn new_run_id() -> RunId {
    RunId::new(Uuid::now_v7().as_u128())
}

/// The text form of a run id, as stored in the database.
pub(crate) fn run_id_text(run: RunId) -> String {
    Uuid::from_u128(run.as_u128()).to_string()
}

/// Reads a run id back from its text form.
pub(crate) fn parse_run_id(text: &str) -> Option<RunId> {
    Uuid::parse_str(text)
        .ok()
        .map(|id| RunId::new(id.as_u128()))
}

/// An instant as `2026-10-26 03:00:00` in the local time zone of whoever
/// looks at it, for the CLI and the TUI.
pub(crate) fn format_local(millis: i64) -> String {
    jiff::Timestamp::from_millisecond(millis).map_or_else(
        |_| millis.to_string(),
        |ts| {
            ts.to_zoned(jiff::tz::TimeZone::system())
                .strftime("%Y-%m-%d %H:%M:%S")
                .to_string()
        },
    )
}

/// A span of milliseconds as `03:41` or `1:02:03`.
pub(crate) fn format_span(millis: i64) -> String {
    let seconds = millis.max(0) / 1_000;
    let (h, m, s) = (seconds / 3_600, seconds / 60 % 60, seconds % 60);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

/// Parses the `--at` of a manual run: RFC 3339 with an offset, or a local
/// date and time read in `zone`.
pub(crate) fn parse_instant(text: &str, zone: &jiff::tz::TimeZone) -> Result<i64> {
    if let Ok(ts) = text.parse::<jiff::Timestamp>() {
        return Ok(ts.as_millisecond());
    }
    let local: jiff::civil::DateTime = text
        .parse()
        .with_context(|| format!("`{text}` is not a date and time such as 2026-10-01T03:00"))?;
    let zoned = zone
        .to_zoned(local)
        .with_context(|| format!("`{text}` cannot be read in the time zone of the DAG"))?;
    Ok(zoned.timestamp().as_millisecond())
}

/// Creates the data directory of a master or a worker, for its user only:
/// the database, the logs and the journal hold what tasks print and the
/// environment they get. A directory that is already there is left as it
/// is, with a warning if others can look inside.
pub(crate) fn private_dir(dir: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
    if let Ok(meta) = std::fs::metadata(dir) {
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            tracing::warn!(
                "{} can be read by other users of this machine (mode {mode:o}): \
                 `chmod 700` it unless that is intended",
                dir.display()
            );
        }
        return Ok(());
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("cannot create the data directory {}", dir.display()))
}

/// Whether two texts are equal, in a time that does not depend on where
/// they differ: `==` stops at the first different byte, and how long a
/// comparison takes would say how much of a secret was right.
pub(crate) fn same_secret(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |differences, (x, y)| differences | (x ^ y))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_compared_whole() {
        assert!(same_secret("sha256:abc", "sha256:abc"));
        assert!(!same_secret("sha256:abc", "sha256:abd"));
        assert!(!same_secret("sha256:abc", "xha256:abc"));
        assert!(!same_secret("sha256:abc", "sha256:ab"));
        assert!(same_secret("", ""));
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3_600));
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(2_592_000)
        );
        assert!(parse_duration("30").is_err());
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("3 weeks").is_err());
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("100MB").unwrap(), Size::Bytes(100 << 20));
        assert_eq!(parse_size("20GB").unwrap(), Size::Bytes(20 << 30));
        assert_eq!(parse_size("10%").unwrap(), Size::Percent(10));
        assert_eq!(Size::Percent(10).of(1_000), 100);
        assert!(parse_size("ten").is_err());
    }

    #[test]
    fn run_ids_survive_the_text_form() {
        let id = new_run_id();
        assert_eq!(parse_run_id(&run_id_text(id)), Some(id));
        assert_eq!(parse_run_id("not-a-uuid"), None);
    }

    #[test]
    fn instants() {
        let rome = jiff::tz::TimeZone::get("Europe/Rome").unwrap();
        let with_offset = parse_instant("2026-10-01T03:00:00+02:00", &rome).unwrap();
        let local = parse_instant("2026-10-01T03:00", &rome).unwrap();
        assert_eq!(with_offset, local);
    }
}
