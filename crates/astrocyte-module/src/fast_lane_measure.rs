//! Opt-in Linux measurements of the real loop with the same binary for both lanes.
use super::*;
use std::cell::Cell;
use std::fs;
use std::process::Command;
use std::sync::atomic::AtomicU64;

static MEASURING: AtomicBool = AtomicBool::new(false);
static FAST_ENABLED: AtomicBool = AtomicBool::new(true);
static FAST_CYCLES: AtomicU64 = AtomicU64::new(0);
static FAST_VM: AtomicU64 = AtomicU64::new(0);
static FAST_BYTES: AtomicU64 = AtomicU64::new(0);
static FULL_CYCLES: AtomicU64 = AtomicU64::new(0);
static FULL_VM: AtomicU64 = AtomicU64::new(0);
static FULL_BYTES: AtomicU64 = AtomicU64::new(0);

thread_local! { static VM_STEPS: Cell<u64> = const { Cell::new(0) }; }

pub(super) fn enabled() -> bool {
    FAST_ENABLED.load(Ordering::Relaxed)
}

pub(super) fn note_vm_steps(steps: u64) {
    if MEASURING.load(Ordering::Relaxed) {
        VM_STEPS.with(|value| value.set(value.get().saturating_add(steps)));
    }
}

fn read_chars() -> (u64, u64) {
    let text = fs::read_to_string("/proc/self/io").unwrap();
    let chars = text
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    (chars, u64::try_from(text.len()).unwrap())
}

pub(super) struct Cycle {
    fast: bool,
    before: u64,
    counter_read_bytes: u64,
}

pub(super) fn cycle(fast: bool) -> Option<Cycle> {
    if !MEASURING.load(Ordering::Relaxed) {
        return None;
    }
    VM_STEPS.with(|value| value.set(0));
    let (before, counter_read_bytes) = read_chars();
    Some(Cycle {
        fast,
        before,
        counter_read_bytes,
    })
}

impl Drop for Cycle {
    fn drop(&mut self) {
        let bytes = read_chars()
            .0
            .saturating_sub(self.before)
            .saturating_sub(self.counter_read_bytes);
        let vm = VM_STEPS.with(Cell::get);
        let (cycles, steps, chars) = if self.fast {
            (&FAST_CYCLES, &FAST_VM, &FAST_BYTES)
        } else {
            (&FULL_CYCLES, &FULL_VM, &FULL_BYTES)
        };
        cycles.fetch_add(1, Ordering::Relaxed);
        steps.fetch_add(vm, Ordering::Relaxed);
        chars.fetch_add(bytes, Ordering::Relaxed);
    }
}

fn cpu_ticks() -> u64 {
    let stat = fs::read_to_string("/proc/self/stat").unwrap();
    let fields = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

fn epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// A quiet multi-reader tree exposes directory traversal costs that a single file hides.
fn inventory_sources(corpus: &mut super::fast_lane_tests::Corpus) {
    use serde_json::{json, Value};
    let inventory = [
        ("claude", 140, 161),
        ("pi", 229, 219),
        ("codex", 45, 41),
        ("gemini", 25, 27),
        ("omp", 5, 12),
        ("kimi", 3, 4),
    ];
    fs::remove_file(&corpus.pi_path).unwrap();
    let old = std::time::SystemTime::now() - Duration::from_secs(600);
    for (name, files, directories) in inventory {
        let root = corpus.directory.path().join(name);
        fs::create_dir_all(&root).unwrap();
        let base = match name {
            "codex" => root.join("sessions"),
            "gemini" => root.join("chats"),
            "kimi" => root.join("group"),
            _ => root.clone(),
        };
        fs::create_dir_all(&base).unwrap();
        let extra = usize::from(base != root);
        let leaves = (0..directories - extra)
            .map(|index| {
                let path = base.join(format!("directory-{index:03}"));
                fs::create_dir(&path).unwrap();
                path
            })
            .collect::<Vec<_>>();
        for index in 0..files {
            let mut value = match name {
                "claude" => {
                    json!({"type":"user","sessionId":format!("session-{index}"),"message":{"role":"user","content":"synthetic"}})
                }
                "codex" => {
                    json!({"type":"session_meta","payload":{"id":format!("session-{index}"),"cwd":"/synthetic"}})
                }
                "gemini" => {
                    json!({"type":"user","role":"user","sessionId":format!("session-{index}")})
                }
                "kimi" => json!({"type":"metadata"}),
                _ => {
                    json!({"type":"session","id":format!("session-{index}"),"cwd":"/synthetic","timestamp":"2026-09-21T00:00:00Z"})
                }
            };
            value["padding"] = Value::from("");
            let target = match index {
                0 => 1024,
                1 => 50 * 1024,
                _ => 1024 + index * 997 % (49 * 1024 + 1),
            };
            let empty = value.to_string().len() + 1;
            value["padding"] = Value::from("x".repeat(target - empty));
            let text = format!("{value}\n");
            assert_eq!(text.len(), target);
            let filename = if name == "kimi" {
                "wire.jsonl".to_string()
            } else {
                format!("session-{index:03}.jsonl")
            };
            let path = leaves[index % leaves.len()].join(filename);
            fs::write(&path, text).unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        let mut found_files = 0;
        let mut found_dirs = 0;
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    found_dirs += 1;
                    pending.push(entry.path());
                } else {
                    found_files += 1;
                    let metadata = entry.metadata().unwrap();
                    assert!((1024..=50 * 1024).contains(&metadata.len()));
                    assert!(metadata.modified().unwrap() <= old);
                }
            }
        }
        assert_eq!((found_files, found_dirs), (files, directories));
        println!("INVENTORY reader={name} files={found_files} descendant_dirs={found_dirs} mtime_age_seconds=600 bytes_min=1024 bytes_max=51200");
    }
    let root = corpus.directory.path();
    corpus.files.claude_code = Some(Arc::new(claude_code_source::ClaudeCodeSource::at(
        root.join("claude"),
    )));
    corpus.files.codex = Some(Arc::new(codex_source::CodexSource::at(root.join("codex"))));
    corpus.files.harnesses = vec![
        Arc::new(gemini_source::GeminiSource::at(root.join("gemini"))),
        Arc::new(omp_source::OmpSource::at(root.join("omp"))),
        Arc::new(kimi_source::KimiSource::at(root.join("kimi"))),
    ];
}

const WRITER: &str = r#"
import json, sqlite3, sys, time
db, path, now = sys.argv[1], sys.argv[2], int(sys.argv[3])
con = sqlite3.connect(db)
start = time.monotonic()
for offset in range(150):
    i = 1001 + offset
    time.sleep(max(0, start + 2 * offset - time.monotonic()))
    data = {'role':'assistant','modelID':'claude-sonnet-4','providerID':'anthropic',
      'time':{'created':now+i,'completed':now+i+1},'finish':'stop',
      'tokens':{'input':3,'output':13,'reasoning':2,'cache':{'read':5,'write':7}}}
    con.execute("INSERT INTO message VALUES(?,'oc-session',?,?,?)", (f'message-{i}',now+i,now+i,json.dumps(data)))
    con.commit()
    line = {'type':'message','id':f'entry-{i}','timestamp':'2026-09-21T00:00:01Z',
      'message':{'role':'assistant','provider':'anthropic','model':'claude-sonnet-4',
        'responseId':f'response-{i}','stopReason':'stop',
        'usage':{'input':11,'output':7,'cacheRead':13,'cacheWrite':17,'reasoning':2,'totalTokens':48}}}
    with open(path, 'a') as f:
        f.write(json.dumps(line) + '\n')
con.close()
"#;

#[test]
#[ignore = "five-minute CPU/I/O measurement; run alone under strace, once per scenario"]
fn measure_fast_lane_five_minutes() {
    let scenario = std::env::var("ASTROCYTE_MEASURE_SCENARIO")
        .expect("set ASTROCYTE_MEASURE_SCENARIO to an idle, active or inventory scenario");
    assert!([
        "idle-full",
        "idle-fast",
        "active-full",
        "active-fast",
        "inventory-idle-full",
        "inventory-idle-fast"
    ]
    .contains(&scenario.as_str()));
    let mut corpus = super::fast_lane_tests::Corpus::new();
    let inventory = scenario.starts_with("inventory-");
    if inventory {
        inventory_sources(&mut corpus);
    }
    let state = Arc::new(crate::state::leased_test_state(&corpus.directory));
    state.set_recent_steps_now_for_test(1_790_000_100_000);
    for index in 1..=1000 {
        corpus.append_opencode(index);
        if !inventory {
            corpus.append_pi(index);
        }
    }
    run_tick(
        &state,
        &corpus.producers,
        &corpus.files,
        None,
        &mut None,
        &mut PricingWalk::new(PricingBudget::PER_TICK),
        &mut QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET),
        &corpus.stop,
    );
    FAST_ENABLED.store(scenario.ends_with("fast"), Ordering::Relaxed);
    let stop = corpus.stop.clone();
    let loop_state = state.clone();
    let loop_stop = stop.clone();
    let thread = std::thread::spawn(move || {
        run_loop_full(
            loop_state,
            corpus.producers,
            corpus.files,
            None,
            None,
            MultiplierStage::new(None),
            30,
            loop_stop,
        )
    });
    crate::test_support::wait_until("initial full tick", || state.interval_waits_for_test() > 0);
    let hz = String::from_utf8(
        Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .parse::<f64>()
    .unwrap();
    let mut writer = scenario.starts_with("active").then(|| {
        Command::new("python3")
            .args(["-c", WRITER])
            .arg(corpus.directory.path().join("opencode.db"))
            .arg(&corpus.pi_path)
            .arg("1790000100000")
            .spawn()
            .unwrap()
    });
    let writer_pid = writer.as_ref().map_or(0, std::process::Child::id);
    let initial_ticks = cpu_ticks();
    let start_ms = epoch_ms();
    println!("MEASURE_START scenario={scenario} pid={} writer_pid={writer_pid} epoch_ms={start_ms} hz={hz}", std::process::id());
    MEASURING.store(true, Ordering::Relaxed);
    for minute in 1..=5 {
        std::thread::sleep(Duration::from_secs(60));
        let ticks = cpu_ticks().saturating_sub(initial_ticks);
        let cpu_seconds = f64::from(u32::try_from(ticks).unwrap()) / hz;
        println!("MEASURE_MINUTE scenario={scenario} minute={minute} cpu_seconds={cpu_seconds:.6}");
    }
    MEASURING.store(false, Ordering::Relaxed);
    let end_ms = epoch_ms();
    stop.store(true, Ordering::Relaxed);
    thread.join().unwrap();
    if let Some(writer) = writer.as_mut() {
        assert!(writer.wait().unwrap().success());
    }
    println!("MEASURE_END scenario={scenario} epoch_ms={end_ms} fast_cycles={} fast_vm={} fast_rchar={} full_cycles={} full_vm={} full_rchar={}", FAST_CYCLES.load(Ordering::Relaxed), FAST_VM.load(Ordering::Relaxed), FAST_BYTES.load(Ordering::Relaxed), FULL_CYCLES.load(Ordering::Relaxed), FULL_VM.load(Ordering::Relaxed), FULL_BYTES.load(Ordering::Relaxed));
}
