"""Run the shared deterministic manifest with the local nzpy_extended driver."""
import asyncio
import json
import os
import resource
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, os.getenv("NZ_PYTHON_DRIVER_ROOT", str(root.parent / "nzpy_extended")))
import nzpy_extended
from nzpy_extended import core


def number(name, default):
    return max(1, int(os.getenv(name, default)))


async def consume(connection, query):
    cursor = connection.cursor()
    rows = cells = 0
    try:
        await cursor.execute(query)
        while batch := await cursor.fetchmany(256):
            rows += len(batch)
            for row in batch:
                cells += len(row)
                for value in row:
                    _ = value
    finally:
        await cursor.close()
    return rows, cells


async def main():
    database = os.getenv("NZ_DEV_DB", os.getenv("NZ_DEV_DATABASE", "JUST_DATA"))
    source = os.getenv("NZ_BENCH_SOURCE_TABLE", f"{database}.ADMIN.FACTPRODUCTINVENTORY")
    limit = number("NZ_BENCH_ROWS", 10000)
    count = number("NZ_BENCH_SAMPLES", 5)
    warmup = number("NZ_BENCH_WARMUP", 1)
    scenarios = json.loads(Path(os.getenv("NZ_BENCH_SCENARIOS", str(root / "benchmarks/netezza_cross_driver/scenarios.json"))).read_text())
    connection = await nzpy_extended.connect(user=os.environ["NZ_DEV_USER"], host=os.environ["NZ_DEV_HOST"], password=os.environ["NZ_DEV_PASSWORD"], port=number("NZ_DEV_PORT", 5480), database=database)
    results = []
    try:
        for scenario in scenarios:
            query = scenario["query"].replace("__SOURCE_TABLE__", source).replace("__ROW_LIMIT__", str(limit))
            repeats = number("NZ_BENCH_TEXT_REPETITIONS", 100) if scenario["name"] == "text-typed-loose" else scenario.get("repetitions", 1)
            for _ in range(warmup):
                await consume(connection, query)
            samples = []
            for _ in range(count):
                start = time.perf_counter()
                cpu = time.process_time()
                rows = cells = 0
                for _ in range(repeats):
                    nr, nc = await consume(connection, query)
                    rows += nr
                    cells += nc
                samples.append(dict(elapsed_ms=(time.perf_counter()-start)*1000, cpu_ms=(time.process_time()-cpu)*1000, rows=rows, cells=cells, peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
            timings = sorted(sample["elapsed_ms"] for sample in samples)
            total = sum(timings)
            results.append(dict(name=scenario["name"], samples=samples, average_ms=total/count, p50_ms=timings[(count-1)//2], p95_ms=timings[round((count-1)*0.95)], rows_per_second=sum(s["rows"] for s in samples)/(total/1000), cells_per_second=sum(s["cells"] for s in samples)/(total/1000)))
    finally:
        await connection.close()
    output = Path(sys.argv[1] if len(sys.argv) > 1 else root / "target/netezza-cross-benchmark/python.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dict(driver="python", version=nzpy_extended.__version__, c_extension=bool(core._HAVE_C_EXT), rows_limit=limit, samples=count, warmup=warmup, results=results), indent=2))
    print(f"saved {output}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"Python benchmark failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
