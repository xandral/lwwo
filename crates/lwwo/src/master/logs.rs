//! The logs of the tasks, on the master's disk.
//!
//! Workers send the output of every attempt in chunks. It is appended to
//! one plain file per attempt, outside SQLite:
//! `logs/<dag>/<run>/<task>/<attempt>.part` while the attempt runs, renamed
//! to `<attempt>.log` when it ends. Beyond the limit per attempt only the
//! beginning and the end are kept. Logs go away with their runs, when
//! `retention` deletes them.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tokio::sync::Notify;

/// The most bytes [`LogStore::read`] returns at once.
const READ_PIECE: u64 = 4 << 20;

/// Written where the middle of a log was dropped.
const TRUNCATION_MARK: &[u8] = b"\n[lwwo: log over the limit, middle part dropped]\n";

/// One attempt of a task of a run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LogKey {
    pub(crate) dag: String,
    pub(crate) run: String,
    pub(crate) task: String,
    pub(crate) attempt: u32,
}

/// What is known of a log while its attempt runs.
#[derive(Debug, Default)]
struct Open {
    /// Bytes received from the worker so far: where the next chunk starts.
    received: u64,
    /// Bytes in the head file, the beginning of the log.
    head: u64,
    /// Bytes in the current tail file.
    tail: u64,
    /// Whether an older tail was thrown away.
    dropped: bool,
    /// Size and truncation, once the log is closed.
    done: Option<(u64, bool)>,
}

/// All the logs of a master.
#[derive(Debug)]
pub(crate) struct LogStore {
    dir: PathBuf,
    /// Limit per attempt: half for the beginning, half for the end.
    max: u64,
    open: Mutex<HashMap<LogKey, Open>>,
    /// Wakes those who follow a log when any log grows.
    grew: Notify,
}

fn append(path: &Path, data: &[u8]) -> io::Result<()> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(data)
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |meta| meta.len())
}

impl LogStore {
    /// The logs under `dir`, with at most `max` bytes kept per attempt.
    pub(crate) fn new(dir: PathBuf, max: u64) -> Self {
        Self {
            dir,
            max: max.max(2),
            open: Mutex::new(HashMap::new()),
            grew: Notify::new(),
        }
    }

    fn base(&self, key: &LogKey) -> PathBuf {
        self.dir
            .join(&key.dag)
            .join(&key.run)
            .join(&key.task)
            .join(key.attempt.to_string())
    }

    /// The beginning of a log that is still being written.
    fn head_path(&self, key: &LogKey) -> PathBuf {
        self.base(key).with_extension("part")
    }

    fn tail_path(&self, key: &LogKey) -> PathBuf {
        self.base(key).with_extension("tail")
    }

    fn old_tail_path(&self, key: &LogKey) -> PathBuf {
        self.base(key).with_extension("tail.old")
    }

    /// The whole log, once the attempt is over.
    fn closed_path(&self, key: &LogKey) -> PathBuf {
        self.base(key).with_extension("log")
    }

    /// Appends a chunk that starts at `offset` of the attempt's output and
    /// returns how many bytes the master now has, for the acknowledgement.
    /// A chunk that does not start where the log ends is not written: the
    /// worker continues from the returned position. With `last` the log is
    /// closed.
    pub(crate) fn append(
        &self,
        key: &LogKey,
        offset: u64,
        data: &[u8],
        last: bool,
    ) -> io::Result<u64> {
        let mut open = self.open.lock().expect("the log lock is never poisoned");
        if !open.contains_key(key) {
            // First chunk since this master started: continue what an
            // earlier one left on disk, if anything.
            let mut state = Open::default();
            if self.closed_path(key).exists() {
                state.done = Some((0, false));
            } else {
                state.head = file_len(&self.head_path(key));
                state.tail = file_len(&self.tail_path(key));
                state.dropped = self.old_tail_path(key).exists();
                state.received = state.head + state.tail;
            }
            open.insert(key.clone(), state);
        }
        let state = open.get_mut(key).expect("just inserted");
        let end = offset + data.len() as u64;
        if state.done.is_some() {
            // Closed already: tell the worker it can forget the rest.
            return Ok(end.max(state.received));
        }
        if offset != state.received {
            return Ok(state.received);
        }

        if !data.is_empty() {
            fs::create_dir_all(self.base(key).parent().expect("logs have a directory"))?;
            let half = self.max / 2;
            let to_head = usize::try_from(half.saturating_sub(state.head))
                .unwrap_or(usize::MAX)
                .min(data.len());
            if to_head > 0 {
                append(&self.head_path(key), &data[..to_head])?;
                state.head += to_head as u64;
            }
            let rest = &data[to_head..];
            if !rest.is_empty() {
                if state.tail > half {
                    // Two tail files of half the limit each always hold the
                    // last half; anything older goes.
                    fs::rename(self.tail_path(key), self.old_tail_path(key))?;
                    state.tail = 0;
                    state.dropped = true;
                }
                append(&self.tail_path(key), rest)?;
                state.tail += rest.len() as u64;
            }
            state.received = end;
        }
        if last {
            state.done = Some(self.close(key, state.dropped)?);
        }
        drop(open);
        self.grew.notify_waiters();
        Ok(end)
    }

    /// Closes a log: the file that was being written becomes the log. If
    /// the output went over the limit, the end that was kept is joined to
    /// the beginning, with a line that says the middle is missing.
    ///
    /// Logs stay as plain text. They are read far more often than they
    /// are large, a plain file can be read from any point without
    /// decoding what comes before, and `retention` deletes them with their
    /// runs anyway.
    fn close(&self, key: &LogKey, dropped: bool) -> io::Result<(u64, bool)> {
        fs::create_dir_all(self.base(key).parent().expect("logs have a directory"))?;
        let head = self.head_path(key);
        let tails = [self.old_tail_path(key), self.tail_path(key)];
        let end: u64 = tails.iter().map(|path| file_len(path)).sum();
        let half = self.max / 2;
        let truncated = dropped || end > half;
        if end > 0 {
            let mut log = OpenOptions::new().create(true).append(true).open(&head)?;
            // The two tail files one after the other, as one stream.
            let mut rest: Box<dyn Read> = Box::new(io::empty());
            for path in &tails {
                if let Ok(file) = File::open(path) {
                    rest = Box::new(rest.chain(file));
                }
            }
            let mut rest = io::BufReader::new(rest);
            if truncated {
                log.write_all(TRUNCATION_MARK)?;
                let skip = end.saturating_sub(half);
                if skip > 0 {
                    io::copy(&mut (&mut rest).take(skip), &mut io::sink())?;
                    // Start the kept end at a line, not in the middle of one.
                    io::BufRead::read_until(&mut rest, b'\n', &mut Vec::new())?;
                }
            }
            io::copy(&mut rest, &mut log)?;
        }
        if !head.exists() {
            // An attempt that wrote nothing still has a log: an empty one.
            File::create(&head)?;
        }
        fs::rename(&head, self.closed_path(key))?;
        for part in tails {
            let _ = fs::remove_file(part);
        }
        Ok((file_len(&self.closed_path(key)), truncated))
    }

    /// Size and truncation of a closed log, for `task_attempts`; forgets
    /// the log. `None` if it was never closed, as for a lost attempt.
    pub(crate) fn take_stats(&self, key: &LogKey) -> Option<(u64, bool)> {
        let mut open = self.open.lock().expect("the log lock is never poisoned");
        match open.get(key) {
            Some(state) if state.done.is_some() => open.remove(key).and_then(|state| state.done),
            Some(_) => {
                // The attempt ended without closing its log: close it now.
                let dropped = open.remove(key).is_some_and(|state| state.dropped);
                self.close(key, dropped).ok()
            }
            None => None,
        }
    }

    /// Reads a log from byte `from`. Also says whether the log is closed:
    /// if not, more may come.
    ///
    /// At most [`READ_PIECE`] bytes come back at a time, so that a large
    /// log is never held in memory whole: the caller asks again from where
    /// it got to, until a piece comes back empty. Each piece is one seek.
    pub(crate) fn read(&self, key: &LogKey, from: u64) -> io::Result<(Vec<u8>, bool)> {
        let mut content = Vec::new();
        let (mut file, closed) = match File::open(self.closed_path(key)) {
            Ok(file) => (file, true),
            Err(_) => match File::open(self.head_path(key)) {
                Ok(file) => (file, false),
                Err(_) => return Ok((content, false)),
            },
        };
        file.seek(SeekFrom::Start(from))?;
        file.take(READ_PIECE).read_to_end(&mut content)?;
        Ok((content, closed))
    }

    /// Waits until some log grows or closes.
    pub(crate) async fn changed(&self) {
        self.grew.notified().await;
    }

    /// Deletes the logs of a run, when retention deletes the run.
    pub(crate) fn delete_run(&self, dag: &str, run: &str) {
        let _ = fs::remove_dir_all(self.dir.join(dag).join(run));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(max: u64) -> (LogStore, PathBuf) {
        let dir = std::env::temp_dir().join(format!("lwwo-logs-{}", uuid::Uuid::new_v4()));
        (LogStore::new(dir.clone(), max), dir)
    }

    fn key() -> LogKey {
        LogKey {
            dag: "etl".to_owned(),
            run: "r1".to_owned(),
            task: "clean".to_owned(),
            attempt: 2,
        }
    }

    #[test]
    fn chunks_are_appended_in_order_and_the_log_is_closed() {
        let (logs, dir) = store(1 << 20);
        assert_eq!(logs.append(&key(), 0, b"one\n", false).unwrap(), 4);
        assert_eq!(logs.read(&key(), 0).unwrap(), (b"one\n".to_vec(), false));
        // The same chunk again, and one from the future: neither is written.
        assert_eq!(logs.append(&key(), 0, b"one\n", false).unwrap(), 4);
        assert_eq!(logs.append(&key(), 9, b"lost\n", false).unwrap(), 4);
        assert_eq!(logs.append(&key(), 4, b"two\n", true).unwrap(), 8);

        assert_eq!(
            logs.read(&key(), 0).unwrap(),
            (b"one\ntwo\n".to_vec(), true)
        );
        assert_eq!(logs.read(&key(), 4).unwrap().0, b"two\n");
        assert!(dir.join("etl/r1/clean/2.log").exists());
        assert!(!dir.join("etl/r1/clean/2.part").exists());
        assert_eq!(logs.take_stats(&key()), Some((8, false)));
        assert_eq!(logs.take_stats(&key()), None, "taken once");
        // A late chunk for the closed log is acknowledged and dropped.
        assert_eq!(logs.append(&key(), 8, b"late\n", true).unwrap(), 13);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn over_the_limit_the_beginning_and_the_end_are_kept() {
        let (logs, dir) = store(200);
        let mut offset = 0;
        for i in 0..100 {
            let line = format!("line {i:03}\n");
            offset = logs
                .append(&key(), offset, line.as_bytes(), i == 99)
                .unwrap();
        }
        let (content, closed) = logs.read(&key(), 0).unwrap();
        let text = String::from_utf8(content).unwrap();
        assert!(closed);
        assert!(text.starts_with("line 000\n"));
        assert!(text.ends_with("line 099\n"));
        assert!(text.contains("middle part dropped"));
        assert!(!text.contains("line 050"));
        let (bytes, truncated) = logs.take_stats(&key()).unwrap();
        assert!(truncated);
        assert!(bytes < 400, "kept {bytes} bytes");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_large_log_is_read_in_pieces() {
        let (logs, dir) = store(u64::MAX);
        let piece = usize::try_from(READ_PIECE).unwrap();
        let big = vec![b'x'; piece * 2 + 10];
        logs.append(&key(), 0, &big, false).unwrap();
        for closed in [false, true] {
            if closed {
                logs.append(&key(), big.len() as u64, b"", true).unwrap();
            }
            let mut position = 0;
            let mut sizes = Vec::new();
            loop {
                let (data, is_closed) = logs.read(&key(), position).unwrap();
                assert_eq!(is_closed, closed);
                if data.is_empty() {
                    break;
                }
                sizes.push(data.len());
                position += data.len() as u64;
            }
            assert_eq!(sizes, [piece, piece, 10]);
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_restarted_master_continues_the_file() {
        let (logs, dir) = store(1 << 20);
        logs.append(&key(), 0, b"before\n", false).unwrap();
        let restarted = LogStore::new(dir.clone(), 1 << 20);
        // The worker does not know where the master is: it is told.
        assert_eq!(restarted.append(&key(), 0, b"", false).unwrap(), 7);
        assert_eq!(restarted.append(&key(), 7, b"after\n", true).unwrap(), 13);
        assert_eq!(restarted.read(&key(), 0).unwrap().0, b"before\nafter\n");
        restarted.delete_run("etl", "r1");
        assert!(!dir.join("etl/r1").exists());
        fs::remove_dir_all(dir).unwrap();
    }
}
