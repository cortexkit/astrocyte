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
        .expect("idle-full, idle-fast, active-full or active-fast");
    assert!(["idle-full", "idle-fast", "active-full", "active-fast"].contains(&scenario.as_str()));
    let corpus = super::fast_lane_tests::Corpus::new();
    let state = Arc::new(crate::state::leased_test_state(&corpus.directory));
    state.set_recent_steps_now_for_test(1_790_000_100_000);
    for index in 1..=1000 {
        corpus.append_opencode(index);
        corpus.append_pi(index);
    }
    corpus.full(&state, 1_790_000_100_000);
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
