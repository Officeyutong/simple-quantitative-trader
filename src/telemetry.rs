use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Mutex, OnceLock},
};

use serde::Serialize;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{LogFormat, LoggingConfig};

const MAX_BUFFERED_LOGS: usize = 2_000;
static LOG_BUFFER: OnceLock<LogBuffer> = OnceLock::new();

#[derive(Clone, Debug, Serialize)]
pub struct LogEntry {
    pub cursor: u64,
    pub line: String,
}

#[derive(Clone, Default)]
struct LogBuffer {
    inner: Arc<Mutex<LogBufferInner>>,
}

#[derive(Default)]
struct LogBufferInner {
    next_cursor: u64,
    entries: VecDeque<LogEntry>,
}

impl LogBuffer {
    fn push(&self, text: &str) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            inner.next_cursor = inner.next_cursor.saturating_add(1);
            let cursor = inner.next_cursor;
            inner.entries.push_back(LogEntry {
                cursor,
                line: line.to_owned(),
            });
            while inner.entries.len() > MAX_BUFFERED_LOGS {
                inner.entries.pop_front();
            }
        }
    }

    fn tail(&self, after_cursor: u64, limit: usize) -> Vec<LogEntry> {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .entries
                    .iter()
                    .filter(|entry| entry.cursor > after_cursor)
                    .rev()
                    .take(limit.min(MAX_BUFFERED_LOGS))
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect()
            })
            .unwrap_or_default()
    }
}

struct TeeWriter {
    buffer: LogBuffer,
    bytes: Vec<u8>,
}

impl Write for TeeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        io::stderr().write_all(bytes)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stderr().flush()
    }
}

impl Drop for TeeWriter {
    fn drop(&mut self) {
        self.buffer.push(&String::from_utf8_lossy(&self.bytes));
    }
}

pub fn init(config: &LoggingConfig) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(&config.level)?;
    let buffer = LOG_BUFFER.get_or_init(LogBuffer::default).clone();
    let writer = move || TeeWriter {
        buffer: buffer.clone(),
        bytes: Vec::new(),
    };
    let registry = tracing_subscriber::registry().with(filter);

    match config.format {
        LogFormat::Pretty => registry
            .with(fmt::layer().with_ansi(false).with_writer(writer))
            .try_init()?,
        LogFormat::Json => registry
            .with(fmt::layer().json().with_ansi(false).with_writer(writer))
            .try_init()?,
    }
    Ok(())
}

pub fn tail(after_cursor: u64, limit: usize) -> Vec<LogEntry> {
    LOG_BUFFER
        .get_or_init(LogBuffer::default)
        .tail(after_cursor, limit)
}
