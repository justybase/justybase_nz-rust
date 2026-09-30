//! Repeatable manifest benchmark; reports allocations, first row and RSS without credentials.
use nz_rust::{
    ColumnDesc, NzConnection, NzConnectionConfig, NzResult, NzValue, QueryStreamSink, Row,
};
use serde_json::{json, Value};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
struct Consumer {
    rows: u64,
    cells: u64,
    start: Instant,
    first_ms: Option<f64>,
}
impl QueryStreamSink for Consumer {
    fn on_columns(&mut self, _: usize, _: &[ColumnDesc], _: Option<&[bool]>) -> NzResult<()> {
        Ok(())
    }
    fn on_row(&mut self, index: usize, row: Row) -> NzResult<()> {
        self.on_values(index, row.columns(), row.try_values()?)
    }
    fn on_values(&mut self, _: usize, _: &[ColumnDesc], values: &[NzValue]) -> NzResult<()> {
        self.first_ms
            .get_or_insert_with(|| self.start.elapsed().as_secs_f64() * 1000.);
        self.rows += 1;
        self.cells += values.len() as u64;
        std::hint::black_box(values);
        Ok(())
    }
}
fn env_number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let output = args
        .windows(2)
        .find(|p| p[0] == "--output")
        .map(|p| p[1].clone())
        .unwrap_or_else(|| "target/netezza-cross-benchmark/rust.json".into());
    if args.iter().any(|arg| arg == "--numeric-replay") {
        return numeric_replay(&output);
    }
    let database = std::env::var("NZ_DEV_DB")
        .or_else(|_| std::env::var("NZ_DEV_DATABASE"))
        .unwrap_or_else(|_| "JUST_DATA".into());
    let mut config = NzConnectionConfig::new(
        &std::env::var("NZ_DEV_HOST")?,
        &database,
        &std::env::var("NZ_DEV_USER")?,
        &std::env::var("NZ_DEV_PASSWORD")?,
    );
    config.port = env_number("NZ_DEV_PORT", 5480).try_into()?;
    let source = std::env::var("NZ_BENCH_SOURCE_TABLE")
        .unwrap_or_else(|_| format!("{database}.ADMIN.FACTPRODUCTINVENTORY"));
    if !source.split('.').all(|part| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }) {
        return Err("invalid NZ_BENCH_SOURCE_TABLE identifier".into());
    }
    let path = std::env::var("NZ_BENCH_SCENARIOS")
        .unwrap_or_else(|_| "benchmarks/netezza_cross_driver/scenarios.json".into());
    let scenarios: Vec<Value> = serde_json::from_slice(&std::fs::read(path)?)?;
    let samples = env_number("NZ_BENCH_SAMPLES", 5);
    let warmup = env_number("NZ_BENCH_WARMUP", 1);
    let limit = env_number("NZ_BENCH_ROWS", 10000);
    let streaming = std::env::var("NZ_BENCH_MODE").as_deref() == Ok("stream");
    let ticks_per_second: f64 = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|r| String::from_utf8(r.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(100.);
    let mut connection = NzConnection::connect(&config)?;
    let mut results = Vec::new();
    for scenario in scenarios {
        let name = scenario["name"].as_str().ok_or("scenario has no name")?;
        let query = scenario["query"]
            .as_str()
            .ok_or("scenario has no query")?
            .replace("__SOURCE_TABLE__", &source)
            .replace("__ROW_LIMIT__", &limit.to_string());
        let repetitions = if name == "text-typed-loose" {
            env_number("NZ_BENCH_TEXT_REPETITIONS", 100)
        } else {
            scenario["repetitions"].as_u64().unwrap_or(1) as usize
        };
        let consume = |connection: &mut NzConnection, repetitions: usize| -> NzResult<Consumer> {
            let mut sink = Consumer {
                rows: 0,
                cells: 0,
                start: Instant::now(),
                first_ms: None,
            };
            for _ in 0..repetitions {
                if streaming {
                    connection.execute_stream(&query, &[], &mut sink)?;
                } else {
                    let result = connection.query(&query, &[])?;
                    for (index, set) in result.result_sets.iter().enumerate() {
                        for row in &set.rows {
                            sink.on_values(index, row.columns(), row.try_values()?)?;
                        }
                    }
                }
            }
            Ok(sink)
        };
        for _ in 0..warmup {
            consume(&mut connection, 1)?;
        }
        let mut timings = Vec::new();
        let mut observations = Vec::new();
        let mut total_rows = 0;
        let mut total_cells = 0;
        for _ in 0..samples {
            let ticks = cpu_ticks();
            ALLOCS.store(0, Ordering::Relaxed);
            BYTES.store(0, Ordering::Relaxed);
            let start = Instant::now();
            let sink = consume(&mut connection, repetitions)?;
            let ms = start.elapsed().as_secs_f64() * 1000.;
            let allocations = ALLOCS.load(Ordering::Relaxed);
            let allocated_bytes = BYTES.load(Ordering::Relaxed);
            let cpu_ms = ticks
                .zip(cpu_ticks())
                .map(|(a, b)| (b - a) as f64 * 1000. / ticks_per_second);
            total_rows += sink.rows;
            total_cells += sink.cells;
            timings.push(ms);
            observations.push(json!({"ms":ms,"rows":sink.rows,"cells":sink.cells,"allocations":allocations,"allocated_bytes":allocated_bytes,"first_row_ms":sink.first_ms,"cpu_ms":cpu_ms,"peak_rss_kib":rss_kib()}));
        }
        timings.sort_by(f64::total_cmp);
        let total_ms: f64 = timings.iter().sum();
        results.push(json!({"name":name,"average_ms":total_ms/samples as f64,"p50_ms":timings[(samples-1)/2],"p95_ms":timings[((samples-1) as f64*0.95).round() as usize],"rows_per_second":total_rows as f64/(total_ms/1000.),"cells_per_second":total_cells as f64/(total_ms/1000.),"observations":observations}));
    }
    connection.close();
    let path = std::path::Path::new(&output);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(
            &json!({"driver":"rust","version":env!("CARGO_PKG_VERSION"),"mode":if streaming {"stream"} else {"materialized"},"rows_limit":limit,"samples":samples,"warmup":warmup,"results":results}),
        )?,
    )?;
    println!("saved {output}");
    Ok(())
}

fn numeric_replay(output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let iterations = env_number("NZ_BENCH_NUMERIC_ITERATIONS", 200000);
    let samples = env_number("NZ_BENCH_NUMERIC_SAMPLES", 5);
    let warmup = env_number("NZ_BENCH_NUMERIC_WARMUP", 10000);
    let path = std::env::var("NZ_BENCH_NUMERIC_CASES")
        .unwrap_or_else(|_| "benchmarks/netezza_cross_driver/numeric_cases.json".into());
    let cases: Vec<Value> = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut results = Vec::new();
    for case in cases {
        let value: nz_rust::NzNumeric = case["value"]
            .as_str()
            .ok_or("missing numeric value")?
            .parse()?;
        let precision = case["precision"].as_i64().ok_or("missing precision")? as i32;
        let scale = case["scale"].as_i64().ok_or("missing scale")? as i32;
        let count = case["digit_count"].as_i64().ok_or("missing digit count")? as i32;
        if !(1..=4).contains(&count) || value.scale() != scale as u32 {
            return Err("invalid numeric case".into());
        }
        let mut bytes = Vec::new();
        for index in (0..count).rev() {
            bytes.extend_from_slice(&((value.coefficient() >> (index * 32)) as u32).to_le_bytes());
        }
        let decode = || nz_rust::types::numeric::get_cs_numeric(&bytes, precision, scale, count);
        let result = decode()?.into_canonical();
        for _ in 0..warmup {
            std::hint::black_box(decode()?);
        }
        let mut timings = Vec::new();
        for _ in 0..samples {
            let start = Instant::now();
            for _ in 0..iterations {
                std::hint::black_box(decode()?);
            }
            timings.push(start.elapsed().as_nanos() as f64 / iterations as f64);
        }
        timings.sort_by(f64::total_cmp);
        results.push(json!({"name":case["name"],"precision":precision,"scale":scale,"digit_count":count,"result":result,"average_ns_per_op":timings.iter().sum::<f64>()/samples as f64,"p50_ns_per_op":timings[(samples-1)/2],"p95_ns_per_op":timings[((samples-1) as f64*0.95).round() as usize]}));
    }
    let path = std::path::Path::new(output);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(
            &json!({"driver":"rust","iterations":iterations,"warmup":warmup,"cases":results}),
        )?,
    )?;
    println!("saved {output}");
    Ok(())
}
