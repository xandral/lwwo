//! The output of an attempt on the worker's disk, until the master has it.
//!
//! The shim writes the output in segments, files named after the position
//! of their first byte in the output: `log.0`, `log.536870912`, … The
//! first segment, the beginning, grows to half of `log_spool_max`; the
//! others to an eighth each. The shipper sends the segments in order and
//! deletes each one once the master has it all, except the one being
//! written. When the segments on disk would go over `log_spool_max`, the
//! shim deletes the oldest one after the beginning: the master has not
//! received it, and sees a jump in the positions where it was.
//!
//! So beyond the limit the beginning and the end of the output are kept,
//! and the limit counts only what the master does not have yet.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// One segment of the output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Segment {
    /// Position of its first byte in the output.
    pub(crate) start: u64,
    /// Bytes in it.
    pub(crate) len: u64,
}

impl Segment {
    /// Position just after its last byte.
    pub(crate) fn end(&self) -> u64 {
        self.start + self.len
    }
}

fn path(dir: &Path, start: u64) -> PathBuf {
    dir.join(format!("log.{start}"))
}

/// The segments in a directory, in order.
pub(crate) fn segments(dir: &Path) -> Vec<Segment> {
    let mut found: Vec<Segment> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let start = entry
                .file_name()
                .to_str()?
                .strip_prefix("log.")?
                .parse()
                .ok()?;
            let len = entry.metadata().ok()?.len();
            Some(Segment { start, len })
        })
        .collect();
    found.sort_by_key(|segment| segment.start);
    found
}

/// Reads at most `max` bytes from position `from` of the output.
///
/// If the bytes at `from` are gone, the piece starts at the next segment:
/// the returned position is then greater than `from`. Also returns where
/// the output on disk ends.
pub(crate) fn read(dir: &Path, from: u64, max: usize) -> (u64, Vec<u8>, u64) {
    let all = segments(dir);
    let end = all.last().map_or(from, Segment::end);
    let mut data = Vec::new();
    let Some(segment) = all.iter().find(|segment| segment.end() > from) else {
        return (from, data, end);
    };
    let at = from.max(segment.start);
    if let Ok(mut file) = File::open(path(dir, segment.start)) {
        if file.seek(SeekFrom::Start(at - segment.start)).is_ok() {
            let _ = file.take(max as u64).read_to_end(&mut data);
        }
    }
    (at, data, end)
}

/// Deletes the segments the master has whole, except the last one, which
/// the shim may still be writing.
pub(crate) fn forget_received(dir: &Path, received: u64) {
    let all = segments(dir);
    if let Some((_, older)) = all.split_last() {
        for segment in older.iter().filter(|segment| segment.end() <= received) {
            let _ = fs::remove_file(path(dir, segment.start));
        }
    }
}

/// Deletes all segments.
pub(crate) fn remove(dir: &Path) {
    for segment in segments(dir) {
        let _ = fs::remove_file(path(dir, segment.start));
    }
}

/// The shim's side: appends lines, keeping the segments within the limit.
#[derive(Debug)]
pub(crate) struct Writer {
    dir: PathBuf,
    max: u64,
    /// The segments written so far, the last one open in `file`; some may
    /// have been deleted by the shipper since.
    written: Vec<Segment>,
    file: Option<File>,
}

impl Writer {
    /// A writer for the output in `dir`, with at most `max` bytes on disk.
    pub(crate) fn new(dir: &Path, max: u64) -> Self {
        Self {
            dir: dir.to_owned(),
            max: max.max(8),
            written: Vec::new(),
            file: None,
        }
    }

    /// Appends one line. A line is never split between segments.
    ///
    /// # Errors
    ///
    /// If a segment cannot be created or written.
    pub(crate) fn line(&mut self, line: &[u8]) -> io::Result<()> {
        let len = line.len() as u64;
        let full = self.written.last().is_none_or(|last| {
            let limit = if last.start == 0 {
                self.max / 2
            } else {
                self.max / 8
            };
            last.len > 0 && last.len + len > limit
        });
        if full {
            let start = self.written.last().map_or(0, Segment::end);
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path(&self.dir, start))?,
            );
            self.written.push(Segment { start, len: 0 });
        }
        self.make_room(len);
        self.file
            .as_mut()
            .expect("a segment is open")
            .write_all(line)?;
        self.written.last_mut().expect("a segment is open").len += len;
        Ok(())
    }

    /// Deletes the oldest segments after the beginning until `len` more
    /// bytes fit. The beginning and the open segment always stay, so a
    /// line longer than the limit is still written.
    fn make_room(&mut self, len: u64) {
        let on_disk = |written: &[Segment]| written.iter().map(|segment| segment.len).sum::<u64>();
        if on_disk(&self.written) + len <= self.max {
            return;
        }
        // The shipper may have made room already.
        let dir = &self.dir;
        let last = self.written.len() - 1;
        let mut index = 0;
        self.written.retain(|segment| {
            index += 1;
            index - 1 == last || path(dir, segment.start).exists()
        });
        while on_disk(&self.written) + len > self.max {
            let open = self.written.len() - 1;
            let Some(oldest) = (0..open).find(|&index| self.written[index].start != 0) else {
                break;
            };
            let segment = self.written.remove(oldest);
            let _ = fs::remove_file(path(&self.dir, segment.start));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lwwo-spool-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn line(n: u32) -> Vec<u8> {
        format!("line {n:04}\n").into_bytes()
    }

    /// Everything on disk, segment by segment.
    fn on_disk(dir: &Path) -> Vec<(u64, Vec<u8>)> {
        segments(dir)
            .into_iter()
            .map(|segment| (segment.start, fs::read(path(dir, segment.start)).unwrap()))
            .collect()
    }

    #[test]
    fn within_the_limit_there_is_one_segment() {
        let dir = temp();
        let mut writer = Writer::new(&dir, 1 << 20);
        writer.line(b"a\n").unwrap();
        writer.line(b"b\n").unwrap();
        assert_eq!(on_disk(&dir), [(0, b"a\nb\n".to_vec())]);
        assert_eq!(read(&dir, 0, 100), (0, b"a\nb\n".to_vec(), 4));
        assert_eq!(read(&dir, 2, 100), (2, b"b\n".to_vec(), 4));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn over_the_limit_the_beginning_and_the_end_are_kept() {
        let dir = temp();
        // Lines of 10 bytes; 80 bytes at most: 4 lines of beginning, then
        // segments of one line.
        let mut writer = Writer::new(&dir, 80);
        for n in 0..20 {
            writer.line(&line(n)).unwrap();
        }
        let kept = on_disk(&dir);
        let total: usize = kept.iter().map(|(_, data)| data.len()).sum();
        assert!(total <= 80, "{total} bytes on disk");
        assert_eq!(kept[0], (0, [line(0), line(1), line(2), line(3)].concat()));
        assert_eq!(kept.last().unwrap(), &(190, line(19)), "named by position");
        // Read from the beginning: the gap shows as a jump.
        let (at, data, end) = read(&dir, 40, 100);
        assert!(at > 40);
        assert_eq!(end, 200);
        assert_eq!(data, fs::read(path(&dir, at)).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn what_the_master_has_is_deleted_and_leaves_room() {
        let dir = temp();
        let mut writer = Writer::new(&dir, 80);
        for n in 0..6 {
            writer.line(&line(n)).unwrap();
        }
        // Segments at 0 (four lines), 40 and 50.
        forget_received(&dir, 60);
        assert_eq!(
            segments(&dir),
            [Segment { start: 50, len: 10 }],
            "the open segment stays"
        );
        for n in 6..12 {
            writer.line(&line(n)).unwrap();
        }
        // Nothing the master lacks was dropped.
        let (at, _, end) = read(&dir, 60, 100);
        assert_eq!((at, end), (60, 120));
        fs::remove_dir_all(dir).unwrap();
    }
}
