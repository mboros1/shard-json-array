use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;
use chrono::{DateTime, Utc};
use crossbeam_channel as channel;
use rand::distributions::{Alphanumeric, DistString};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Serialize;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "generate-entities", about = "Generate large JSON entities for stress testing", version)]
struct Args {
    /// Number of entities to generate
    n: u64,

    /// Output file path (JSON)
    #[arg(short, long, default_value = "entities.json")]
    output: PathBuf,

    /// Output as a top-level array instead of {"entities": [...]}
    #[arg(long)]
    array_only: bool,

    /// Optional RNG seed (for reproducibility)
    #[arg(long)]
    seed: Option<u64>,

    /// Number of parallel generator threads (default: CPU count)
    #[arg(short = 'j', long)]
    jobs: Option<usize>,
}

#[derive(Serialize)]
struct Entity {
    id: Uuid,
    kind: &'static str,
    name: String,
    owner: Owner,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    description: String,
    readme: String,
    tags: Vec<String>,
    topics: Vec<String>,
    license: License,
    files: Vec<FileMeta>,
    contributors: Vec<Contributor>,
    urls: Vec<String>,
    permissions: Permissions,
    metadata: Metadata,
    extra: Extra,
}

#[derive(Serialize)]
struct Owner {
    id: Uuid,
    login: String,
    name: String,
    followers: u32,
}

#[derive(Serialize)]
struct License {
    key: &'static str,
    name: &'static str,
    spdx_id: &'static str,
}

#[derive(Serialize)]
struct FileMeta {
    path: String,
    size: u64,
    checksum: String,
    preview: String,
}

#[derive(Serialize)]
struct Contributor {
    id: Uuid,
    login: String,
    commits: u32,
}

#[derive(Serialize)]
struct Permissions {
    admin: bool,
    maintain: bool,
    push: bool,
    triage: bool,
    pull: bool,
}

#[derive(Serialize)]
struct Metadata {
    stars: u32,
    forks: u32,
    watchers: u32,
    issues_open: u32,
    releases: u32,
    languages: Vec<String>,
}

#[derive(Serialize)]
struct Extra {
    attributes: Vec<Attribute>,
    notes: Vec<String>,
}

#[derive(Serialize)]
struct Attribute {
    key: String,
    value: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let workers = args.jobs.unwrap_or_else(num_cpus::get);
    let base_seed: u64 = args.seed.unwrap_or_else(rand::random);

    let file = File::create(&args.output)
        .with_context(|| format!("failed to create {}", args.output.display()))?;
    let mut w = BufWriter::new(file);

    let prefix = if args.array_only { b"[\n" as &[u8] } else { b"{\"entities\":[\n" };
    w.write_all(prefix)?;

    let (work_tx, work_rx) = channel::bounded::<u64>(workers * 4);
    let (res_tx, res_rx) = channel::bounded::<(u64, Vec<u8>)>(workers * 4);

    // Feeder thread: dispatch indices 0..n
    let n = args.n;
    let feeder = std::thread::spawn(move || {
        for i in 0..n {
            if work_tx.send(i).is_err() {
                break;
            }
        }
        // drop sender to close channel
    });

    // Spawn worker threads
    let mut handles = Vec::with_capacity(workers);
    for t in 0..workers {
        let rx = work_rx.clone();
        let tx = res_tx.clone();
        let seed = mix64(base_seed ^ t as u64);
        let handle = std::thread::spawn(move || {
            while let Ok(i) = rx.recv() {
                // Per-entity deterministic RNG; independent of threading
                let mut rng = StdRng::seed_from_u64(mix64(seed ^ i));
                let ent = generate_entity(i, &mut rng);
                let mut buf = Vec::with_capacity(16 * 1024);
                if let Err(_e) = serde_json::to_writer(&mut buf, &ent) {
                    // On serialization error, skip this entity
                    continue;
                }
                if tx.send((i, buf)).is_err() {
                    break;
                }
            }
        });
        handles.push(handle);
    }

    drop(res_tx); // Only worker clones remain

    // Writer: receive out-of-order items, write in-order
    let mut next: u64 = 0;
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

    while next < n {
        let (i, data) = match res_rx.recv() {
            Ok(v) => v,
            Err(_) => bail!("generator workers stopped before producing all items (wrote {} of {})", next, n),
        };
        pending.insert(i, data);
        while let Some(data) = pending.remove(&next) {
            if next > 0 {
                w.write_all(b",\n")?;
            }
            w.write_all(&data)?;
            next += 1;
        }
    }

    // Ensure feeder and workers exit
    let _ = feeder.join();
    for h in handles {
        let _ = h.join();
    }

    let suffix = if args.array_only { b"\n]\n" as &[u8] } else { b"\n]}\n" };
    w.write_all(suffix)?;
    w.flush()?;

    eprintln!(
        "generated {} entities -> {} (array_only: {}, jobs: {})",
        args.n,
        args.output.display(),
        args.array_only,
        workers
    );
    Ok(())
}

fn generate_entity(i: u64, rng: &mut StdRng) -> Entity {
    let now = Utc::now();
    let created_offset = rand_between_days(rng, 1000);
    let updated_offset = rand_between_days(rng, 100);

    let owner = Owner {
        id: Uuid::new_v4(),
        login: wordish(rng, 12),
        name: words(rng, 2, 5),
        followers: rng.gen_range(0..50_000),
    };

    let description = repeat_lorem(rng, 5_000, 9_000);
    let readme = repeat_lorem(rng, 10_000, 18_000);

    let tags = many_words(rng, 40, 65);
    let topics = many_words(rng, 15, 30);

    let files = (0..rng.gen_range(20..40))
        .map(|_| FileMeta {
            path: format!("src/{}/{}.rs", wordish(rng, 6), wordish(rng, 8)),
            size: rng.gen_range(200..10_000),
            checksum: hexish(rng, 40),
            preview: words(rng, 40, 80),
        })
        .collect();

    let contributors = (0..rng.gen_range(5..25))
        .map(|_| Contributor {
            id: Uuid::new_v4(),
            login: wordish(rng, 10),
            commits: rng.gen_range(1..10_000),
        })
        .collect();

    let languages = (0..rng.gen_range(3..10))
        .map(|_| wordish(rng, 6))
        .collect();

    let attributes = (0..rng.gen_range(30..60))
        .map(|_| Attribute {
            key: words(rng, 1, 3),
            value: words(rng, 3, 8),
        })
        .collect();

    let notes = (0..rng.gen_range(10..30))
        .map(|_| repeat_lorem(rng, 200, 800))
        .collect();

    Entity {
        id: Uuid::new_v4(),
        kind: if rng.gen_bool(0.5) { "repository" } else { "service" },
        name: format!("{}-{}-{}", wordish(rng, 6), wordish(rng, 6), i),
        owner,
        created_at: now - created_offset,
        updated_at: now - updated_offset,
        description,
        readme,
        tags,
        topics,
        license: License {
            key: "apache-2.0",
            name: "Apache License 2.0",
            spdx_id: "Apache-2.0",
        },
        files,
        contributors,
        urls: vec![
            format!("https://example.com/{}/{}", wordish(rng, 8), wordish(rng, 8)),
            format!("https://api.example.com/{}/{}", wordish(rng, 8), wordish(rng, 8)),
            format!("https://docs.example.com/{}/{}", wordish(rng, 8), wordish(rng, 8)),
        ],
        permissions: Permissions {
            admin: rng.gen_bool(0.2),
            maintain: rng.gen_bool(0.3),
            push: rng.gen_bool(0.5),
            triage: rng.gen_bool(0.5),
            pull: true,
        },
        metadata: Metadata {
            stars: rng.gen_range(0..100_000),
            forks: rng.gen_range(0..50_000),
            watchers: rng.gen_range(0..100_000),
            issues_open: rng.gen_range(0..10_000),
            releases: rng.gen_range(0..500),
            languages,
        },
        extra: Extra { attributes, notes },
    }
}

fn rand_between_days(rng: &mut StdRng, max_days: i64) -> chrono::Duration {
    let days = rng.gen_range(0..=max_days);
    let secs = rng.gen_range(0..86_400);
    chrono::Duration::days(days as i64) + chrono::Duration::seconds(secs as i64)
}

fn wordish(rng: &mut StdRng, len: usize) -> String {
    Alphanumeric.sample_string(rng, len).to_lowercase()
}

fn words(rng: &mut StdRng, min_words: usize, max_words: usize) -> String {
    let n = rng.gen_range(min_words..=max_words);
    (0..n)
        .map(|_| {
            let len = rng.gen_range(3..12);
            wordish(rng, len)
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn many_words(rng: &mut StdRng, min: usize, max: usize) -> Vec<String> {
    let n = rng.gen_range(min..=max);
    (0..n)
        .map(|_| {
            let len = rng.gen_range(3..12);
            wordish(rng, len)
        })
        .collect()
}

fn hexish(rng: &mut StdRng, len: usize) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let b = rng.gen_range(0..16);
        s.push(HEX[b] as char);
    }
    s
}

fn repeat_lorem(rng: &mut StdRng, min_len: usize, max_len: usize) -> String {
    const LOREM: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed, dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius a, semper congue, euismod non, mi. Proin porttitor, orci nec nonummy molestie, enim est eleifend mi, non fermentum diam nisl sit amet erat. Duis semper. Duis arcu massa, scelerisque vitae, consequat in, pretium a, enim. Pellentesque congue.";
    let target = rng.gen_range(min_len..=max_len);
    let mut s = String::with_capacity(target + 128);
    while s.len() < target {
        s.push_str(LOREM);
        s.push(' ');
    }
    s.truncate(target);
    s
}

#[inline]
fn mix64(mut z: u64) -> u64 {
    // SplitMix64 finalizer, good bit-mixing for seeding
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}
