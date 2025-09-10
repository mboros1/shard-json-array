use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use std::thread;

use anyhow::{bail, Context, Result};
use clap::Parser;
use crossbeam_channel::{unbounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use serde::de::{self, DeserializeSeed, Deserializer as _, MapAccess, SeqAccess, Visitor};
use serde_json::{self, Value};

/// Simple, fast CLI to shard a huge JSON array into N zstd-compressed NDJSON files.
#[derive(Parser, Debug)]
#[command(name = "shard-json-array", version, about = "Shard a large JSON array into N zstd-compressed NDJSON files", disable_help_subcommand = true)]
struct Args {
    /// Path to input JSON file (defaults to expecting a top-level array)
    input_file: PathBuf,
    /// Number of shards to produce
    num_shards: usize,
    /// If set, expect a top-level object and stream the array at this key (e.g. --array-key entities)
    #[arg(long, value_name = "KEY")]
    array_key: Option<String>,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {:#}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    if args.num_shards == 0 {
        bail!("num_shards must be > 0");
    }

    let input_path = args.input_file;
    let shards = args.num_shards;

    // Open input and set up progress.
    let file = File::open(&input_path)
        .with_context(|| format!("failed to open input file: {}", input_path.display()))?;
    let file_size = file.metadata().ok().map(|m| m.len()).unwrap_or(0);

    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} {bar:40.cyan/blue} {bytes}/{total_bytes} • {bytes_per_sec} • {eta}"
        )
        .unwrap()
        .progress_chars("##-"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(80));

    let counting = Arc::new(AtomicU64::new(0));
    let reader = CountingReader::new(file, counting.clone(), Some(pb.clone()));
    let mut reader = BufReader::with_capacity(16 * 1024 * 1024, reader); // 16 MiB buffer

    // Prepare shard workers.
    let width = ((shards as f64).log10().floor() as usize) + 1;
    let (senders, workers) = spawn_shard_workers(&input_path, shards, width)?;

    // Stream-parse the input and dispatch elements.
    let total_items = stream_into_workers(&mut reader, &senders, shards, args.array_key.as_deref())?;

    // Close senders so workers can finish.
    drop(senders);

    // Join workers and gather per-shard counts.
    let mut shard_counts = Vec::with_capacity(workers.len());
    for (i, handle) in workers.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(count)) => shard_counts.push((i, count)),
            Ok(Err(e)) => return Err(e).with_context(|| format!("worker {} failed", i)),
            Err(_) => bail!("worker {} panicked", i),
        }
    }

    pb.finish_with_message("done");

    // Summary to stderr.
    eprintln!(
        "sharded {} items across {} files (input: {})",
        total_items,
        shards,
        input_path.display()
    );
    for (i, c) in shard_counts {
        eprintln!("  shard {:>width$}: {} items", i, c, width = width);
    }

    Ok(())
}

// A Read wrapper that counts bytes and optionally updates a progress bar.
struct CountingReader<R: Read> {
    inner: R,
    counter: Arc<AtomicU64>,
    pb: Option<ProgressBar>,
}

impl<R: Read> CountingReader<R> {
    fn new(inner: R, counter: Arc<AtomicU64>, pb: Option<ProgressBar>) -> Self {
        Self { inner, counter, pb }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            let prev = self.counter.fetch_add(n as u64, Ordering::Relaxed);
            if let Some(pb) = &self.pb {
                // Best-effort; may overcount slightly depending on buffering, which is fine.
                let new_pos = prev + n as u64;
                pb.set_position(new_pos);
            }
        }
        Ok(n)
    }
}

fn spawn_shard_workers(
    input_path: &Path,
    shards: usize,
    width: usize,
) -> Result<(Vec<Sender<Value>>, Vec<thread::JoinHandle<Result<u64>>>)> {
    let mut senders = Vec::with_capacity(shards);
    let mut workers = Vec::with_capacity(shards);

    for i in 0..shards {
        let (tx, rx) = unbounded::<Value>();
        let out_path = output_path_for(input_path, i, shards, width);
        let handle = thread::spawn(move || worker_write_ndjson_zstd(i, &out_path, rx));
        senders.push(tx);
        workers.push(handle);
    }

    Ok((senders, workers))
}

fn output_path_for(input_path: &Path, shard_idx: usize, shards: usize, width: usize) -> PathBuf {
    let parent = input_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = input_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "input".to_string());
    parent.join(format!(
        "{}.shard-{:0width$}-of-{}.ndjson.zst",
        stem,
        shard_idx,
        shards,
        width = width
    ))
}

fn worker_write_ndjson_zstd(idx: usize, out_path: &Path, rx: Receiver<Value>) -> Result<u64> {
    let file = File::create(out_path)
        .with_context(|| format!("failed to create output file: {}", out_path.display()))?;
    let writer = BufWriter::with_capacity(8 * 1024 * 1024, file); // 8 MiB buffer

    // Lower compression level favors throughput.
    let mut encoder = zstd::Encoder::new(writer, 1 /* level */)
        .context("failed to initialize zstd encoder")?;
    // For streamability and interop; do not include content size when unknown.

    let mut count: u64 = 0;
    for value in rx.iter() {
        serde_json::to_writer(&mut encoder, &value).context("failed to serialize JSON element")?;
        encoder
            .write_all(b"\n")
            .context("failed to write newline to output")?;
        count += 1;
    }

    // Ensure buffers are flushed.
    let _ = encoder.finish();
    eprintln!(
        "  wrote shard {} -> {}",
        idx,
        out_path.file_name().unwrap_or_default().to_string_lossy()
    );
    Ok(count)
}

// A serde Visitor that expects a top-level array and streams its elements.
struct StreamArrayVisitor<'a> {
    senders: &'a [Sender<Value>],
}

impl<'de, 'a> Visitor<'de> for StreamArrayVisitor<'a> {
    type Value = u64; // total elements processed

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a top-level JSON array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut idx: u64 = 0;
        let n = self.senders.len();
        while let Some(elem) = seq.next_element::<Value>()? {
            let shard = (idx as usize) % n;
            // If sending fails, the receiver is gone; map to a serde error.
            self.senders[shard]
                .send(elem)
                .map_err(|e| de::Error::custom(format!("send to shard {} failed: {}", shard, e)))?;
            idx += 1;
        }
        Ok(idx)
    }
}

// A DeserializeSeed that consumes an array value and streams its elements.
struct StreamArraySeed<'a> {
    senders: &'a [Sender<Value>],
}

impl<'de, 'a> DeserializeSeed<'de> for StreamArraySeed<'a> {
    type Value = u64;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(StreamArrayVisitor { senders: self.senders })
    }
}

// A root visitor that accepts either a top-level array, or an object with an "entities" array.
struct RootVisitor<'a> {
    senders: &'a [Sender<Value>],
    array_key: Option<&'a str>,
}

impl<'de, 'a> Visitor<'de> for RootVisitor<'a> {
    type Value = u64;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(k) = self.array_key {
            write!(f, "a top-level array or an object with an '{}' array", k)
        } else {
            write!(f, "a top-level JSON array")
        }
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        StreamArrayVisitor { senders: self.senders }.visit_seq(seq)
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        match self.array_key {
            None => {
                // User did not request an object key; reject object input for clarity.
                // Consume the object to avoid partial reads, but return a helpful error.
                while let Some(_k) = map.next_key::<de::IgnoredAny>()? {
                    let _: de::IgnoredAny = map.next_value()?;
                }
                Err(de::Error::custom("got a JSON object but no --array-key was provided"))
            }
            Some(expected) => {
                let mut total: u64 = 0;
                let mut found = false;
                while let Some(key) = map.next_key::<String>()? {
                    if key == expected {
                        let count = map.next_value_seed(StreamArraySeed { senders: self.senders })?;
                        total += count;
                        found = true;
                    } else {
                        let _: de::IgnoredAny = map.next_value()?;
                    }
                }
                if !found {
                    return Err(de::Error::custom(format!(
                        "object did not contain an '{}' array (use --array-key to set the key)",
                        expected
                    )));
                }
                Ok(total)
            }
        }
    }
}

fn stream_into_workers<R: Read>(reader: R, senders: &[Sender<Value>], shards: usize, array_key: Option<&str>) -> Result<u64> {
    if shards == 0 {
        bail!("num_shards must be > 0");
    }
    let mut de = serde_json::Deserializer::from_reader(reader);
    let visitor = RootVisitor { senders, array_key };
    let total: u64 = de
        .deserialize_any(visitor)
        .context("failed to stream JSON input")?;

    Ok(total)
}
