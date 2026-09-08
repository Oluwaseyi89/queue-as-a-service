//! A generic, crash-safe, append-only write-ahead log — see [`Wal`] for
//! the actual design. [`queue::PersistentFifoQueue`](crate::queue::PersistentFifoQueue)
//! and [`queue::PersistentPriorityQueue`](crate::queue::PersistentPriorityQueue)
//! are the concrete uses of it in this crate today, but the type itself
//! doesn't know anything about queues — it's a log of arbitrary
//! serializable records, reusable wherever else this crate ends up
//! needing one (a Raft log, for one).

use std::io;
use std::marker::PhantomData;
use std::path::Path;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Bytes in a record's length-prefix field.
const LENGTH_PREFIX_BYTES: usize = 4;
/// Bytes in a record's checksum field.
const CHECKSUM_BYTES: usize = 4;

/// A durable, append-only log of records.
///
/// Each record is framed on disk as `[u32 length][u32 crc32][payload]`,
/// fsynced before [`append`](Self::append) returns — every append this
/// method has returned `Ok` for is guaranteed to survive a crash, at the
/// cost of a disk sync on every single call. That trade favors
/// correctness over throughput deliberately: this branch's job is making
/// durability exist at all, not making it fast. Batching multiple
/// appends into one sync (group commit) is a real optimization for
/// later, once there's an actual throughput target to hit.
///
/// This is a log, not a general-purpose store: there's no random access,
/// no update, no delete. The only ways to get records out are replaying
/// the whole thing from the start (which [`open`](Self::open) does once,
/// automatically) or appending a new one to the end. That constraint is
/// deliberate — it's also exactly the shape a Raft log needs
/// (`feature/raft-replication`), so this primitive is meant to be reused
/// there, not just for the FIFO/priority queue wrappers built on it here.
pub struct Wal<T> {
    writer: Mutex<File>,
    _record: PhantomData<fn() -> T>,
}

/// What [`read_exact_or_eof`] found when trying to fill a buffer.
enum ReadOutcome {
    /// The buffer was filled completely.
    Full,
    /// Zero bytes were available — a clean end of file.
    Eof,
    /// Some bytes were available, but fewer than the buffer needed, and
    /// then the file ended. This is what a crash mid-write looks like: a
    /// record whose header or payload was only partially flushed before
    /// the process died.
    ShortRead,
}

/// Fills `buf` from `file`, distinguishing a clean EOF (nothing read at
/// all) from a torn one (some bytes read, then EOF) — `AsyncReadExt`'s
/// own `read_exact` collapses that distinction into a single
/// `UnexpectedEof` error, which isn't enough information to tell "this
/// file just ends here" apart from "this file was truncated by a crash
/// mid-record".
async fn read_exact_or_eof(file: &mut File, buf: &mut [u8]) -> io::Result<ReadOutcome> {
    let mut filled = 0;
    while filled < buf.len() {
        let read = file.read(&mut buf[filled..]).await?;
        if read == 0 {
            return Ok(if filled == 0 { ReadOutcome::Eof } else { ReadOutcome::ShortRead });
        }
        filled += read;
    }
    Ok(ReadOutcome::Full)
}

impl<T: Serialize + DeserializeOwned> Wal<T> {
    /// Opens the WAL at `path` (creating it if it doesn't exist yet),
    /// replays every valid record currently in it, and returns both an
    /// open handle ready for further appends and the replayed records in
    /// the order they were originally written.
    ///
    /// Replay and open are one operation, not two, deliberately: a WAL
    /// that could be opened for writing before its existing contents were
    /// read back would make "append before you've replayed" a possible
    /// (and silently corrupting) mistake. Tying them together makes it
    /// impossible to get the order wrong.
    ///
    /// If the tail of the file is torn (a length/checksum header or
    /// payload that's shorter than it claims to be — the signature of a
    /// crash mid-write) or a record's payload doesn't match its stored
    /// checksum, replay stops at the last good record instead of
    /// erroring. Everything before that point is trusted; the file is
    /// then truncated to that point before any new append happens, so
    /// the torn bytes don't sit in the middle of the file corrupting
    /// every future replay from here on.
    ///
    /// # Errors
    ///
    /// Returns an error if the file can't be opened, read, or truncated,
    /// or if a fully-written record fails to deserialize as `T` — the
    /// last case is a genuine bug (a `Wal<T>` used with a `T` other than
    /// what wrote the file) rather than something replay should silently
    /// paper over the way it does for torn writes.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<(Self, Vec<T>)> {
        let path = path.as_ref();
        let (records, valid_len) = Self::replay(path).await?;

        let writer = OpenOptions::new().create(true).append(true).open(path).await?;
        writer.set_len(valid_len).await?;

        Ok((Self { writer: Mutex::new(writer), _record: PhantomData }, records))
    }

    /// Reads every record from the start of the file at `path`, stopping
    /// at the first torn or corrupt one. Returns the valid records and
    /// the byte length of the file up to (not including) that point.
    async fn replay(path: &Path) -> io::Result<(Vec<T>, u64)> {
        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok((Vec::new(), 0));
            }
            Err(error) => return Err(error),
        };

        let mut records = Vec::new();
        let mut valid_len: u64 = 0;

        loop {
            let mut header = [0u8; LENGTH_PREFIX_BYTES + CHECKSUM_BYTES];
            match read_exact_or_eof(&mut file, &mut header).await? {
                ReadOutcome::Eof | ReadOutcome::ShortRead => break,
                ReadOutcome::Full => {}
            }
            let payload_len =
                u32::from_le_bytes(header[..LENGTH_PREFIX_BYTES].try_into().unwrap()) as usize;
            let expected_checksum =
                u32::from_le_bytes(header[LENGTH_PREFIX_BYTES..].try_into().unwrap());

            let mut payload = vec![0u8; payload_len];
            match read_exact_or_eof(&mut file, &mut payload).await? {
                ReadOutcome::Full => {}
                ReadOutcome::Eof | ReadOutcome::ShortRead => break,
            }

            if crc32fast::hash(&payload) != expected_checksum {
                // Bytes exist, but don't match their own checksum — the
                // same conclusion as a torn write: trust nothing from
                // here on.
                break;
            }

            let record: T = serde_json::from_slice(&payload)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            records.push(record);
            valid_len += (header.len() + payload.len()) as u64;
        }

        Ok((records, valid_len))
    }

    /// Appends `record` to the log and fsyncs before returning. On `Ok`,
    /// `record` is durable: a crash immediately after this call returns
    /// will still see it on the next [`open`](Self::open).
    ///
    /// # Errors
    ///
    /// Returns an error if `record` can't be serialized, or if the write
    /// or the fsync that follows it fails.
    pub async fn append(&self, record: &T) -> io::Result<()> {
        let payload = serde_json::to_vec(record)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL record exceeds the 4 GiB length prefix can address",
            )
        })?;
        let checksum = crc32fast::hash(&payload);

        let mut framed = Vec::with_capacity(LENGTH_PREFIX_BYTES + CHECKSUM_BYTES + payload.len());
        framed.extend_from_slice(&payload_len.to_le_bytes());
        framed.extend_from_slice(&checksum.to_le_bytes());
        framed.extend_from_slice(&payload);

        let mut writer = self.writer.lock().await;
        writer.write_all(&framed).await?;
        writer.sync_data().await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    use super::Wal;

    #[tokio::test]
    async fn opening_a_path_that_does_not_exist_yet_creates_it_empty() {
        let dir = tempdir().unwrap();
        let (_wal, records) = Wal::<String>::open(dir.path().join("wal.log")).await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn appended_records_replay_back_in_the_same_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let (wal, _) = Wal::<i32>::open(&path).await.unwrap();
            wal.append(&1).await.unwrap();
            wal.append(&2).await.unwrap();
            wal.append(&3).await.unwrap();
        }

        let (_wal, records) = Wal::<i32>::open(&path).await.unwrap();
        assert_eq!(records, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_corrupted_checksum_truncates_replay_at_the_last_good_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let good_len = {
            let (wal, _) = Wal::<i32>::open(&path).await.unwrap();
            wal.append(&1).await.unwrap();
            wal.append(&2).await.unwrap();
            tokio::fs::metadata(&path).await.unwrap().len()
        };

        // Append a third, well-formed-looking record, then flip a byte in
        // its payload without touching its checksum — a bit-flip on disk,
        // not a truncation, so the length/checksum header alone can't
        // catch it; only comparing the payload against its checksum can.
        {
            let (wal, _) = Wal::<i32>::open(&path).await.unwrap();
            wal.append(&3).await.unwrap();
        }
        {
            let mut file = tokio::fs::OpenOptions::new().write(true).open(&path).await.unwrap();
            // The third record's payload starts right after the two
            // 8-byte headers and two single-byte JSON payloads ("1", "2")
            // that precede it, plus its own 8-byte header.
            let corruption_offset = good_len + 8;
            file.seek(std::io::SeekFrom::Start(corruption_offset)).await.unwrap();
            file.write_all(b"9").await.unwrap(); // "3" -> "9", same length
        }

        let (_wal, records) = Wal::<i32>::open(&path).await.unwrap();
        assert_eq!(records, vec![1, 2]);

        // And replay must have truncated the file back to the last good
        // record, not left the corrupt one sitting in the middle of it.
        let healed_len = tokio::fs::metadata(&path).await.unwrap().len();
        assert_eq!(healed_len, good_len);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_are_all_present_and_individually_intact() {
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let (wal, _) = Wal::<i32>::open(&path).await.unwrap();
        let wal = Arc::new(wal);

        let mut appenders = Vec::new();
        for i in 0..50 {
            let wal = Arc::clone(&wal);
            appenders.push(tokio::spawn(async move {
                wal.append(&i).await.unwrap();
            }));
        }
        for appender in appenders {
            appender.await.unwrap();
        }

        let (_wal, mut records) = Wal::<i32>::open(&path).await.unwrap();
        records.sort_unstable();
        assert_eq!(records, (0..50).collect::<Vec<_>>());
    }
}
