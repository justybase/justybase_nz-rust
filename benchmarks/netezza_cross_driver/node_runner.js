const fs = require('node:fs');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

const driverRoot = process.env.NZ_NODE_DRIVER_ROOT || path.resolve(__dirname, '../../../justybase_netezza_node_driver');
const driverEntry = process.env.NZ_NODE_DRIVER_ENTRY || path.join(driverRoot, 'dist', 'cjs');
const { NzConnection } = require(driverEntry);

const numberEnv = (name, fallback) => {
    const value = Number(process.env[name]);
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
};
const database = process.env.NZ_DEV_DB || process.env.NZ_DEV_DATABASE || 'JUST_DATA';
const sourceTable = process.env.NZ_BENCH_SOURCE_TABLE || `${database}.ADMIN.FACTPRODUCTINVENTORY`;
const rowsLimit = numberEnv('NZ_BENCH_ROWS', 10000);
const sampleCount = numberEnv('NZ_BENCH_SAMPLES', 5);
const warmupCount = numberEnv('NZ_BENCH_WARMUP', 1);
const textRepetitions = numberEnv('NZ_BENCH_TEXT_REPETITIONS', 100);
const output = process.argv[2] || 'target/netezza-cross-benchmark/node.json';

function renderQuery(query) {
    return query.replaceAll('__SOURCE_TABLE__', sourceTable).replaceAll('__ROW_LIMIT__', String(rowsLimit));
}

async function consume(connection, query) {
    const reader = await connection.createCommand(query).executeReader();
    let rows = 0;
    let cells = 0;
    try {
        while (await reader.read()) {
            rows++;
            cells += reader.fieldCount;
            for (let index = 0; index < reader.fieldCount; index++) reader.getValue(index);
        }
    } finally {
        await reader.close();
    }
    return { rows, cells };
}

function percentile(values, p) {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted[Math.round((sorted.length - 1) * p)];
}

async function main() {
    const scenariosPath = process.env.NZ_BENCH_SCENARIOS || path.join(__dirname, 'scenarios.json');
    const scenarios = JSON.parse(fs.readFileSync(scenariosPath, 'utf8')).map((scenario) => ({
        ...scenario,
        query: renderQuery(scenario.query),
        repetitions: scenario.name === 'text-typed-loose' ? textRepetitions : scenario.repetitions,
    }));
    const connection = new NzConnection({
        host: process.env.NZ_DEV_HOST || '127.0.0.1',
        port: Number(process.env.NZ_DEV_PORT || 5480),
        database,
        user: process.env.NZ_DEV_USER || 'admin',
        password: process.env.NZ_DEV_PASSWORD || '',
    });
    await connection.connect();
    const results = [];
    try {
        for (const scenario of scenarios) {
            for (let i = 0; i < warmupCount; i++) await consume(connection, scenario.query);
            const samples = [];
            for (let sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++) {
                const startHeap = process.memoryUsage().heapUsed;
                const start = performance.now();
                let rows = 0;
                let cells = 0;
                for (let repetition = 0; repetition < scenario.repetitions; repetition++) {
                    const result = await consume(connection, scenario.query);
                    rows += result.rows;
                    cells += result.cells;
                }
                samples.push({
                    elapsed_ms: performance.now() - start,
                    rows,
                    cells,
                    allocated_bytes: process.memoryUsage().heapUsed - startHeap,
                });
            }
            const totalMs = samples.reduce((sum, sample) => sum + sample.elapsed_ms, 0);
            const totalRows = samples.reduce((sum, sample) => sum + sample.rows, 0);
            const totalCells = samples.reduce((sum, sample) => sum + sample.cells, 0);
            const elapsed = samples.map((sample) => sample.elapsed_ms);
            results.push({
                name: scenario.name,
                description: scenario.description,
                repetitions: scenario.repetitions,
                samples,
                average_ms: totalMs / samples.length,
                p50_ms: percentile(elapsed, 0.50),
                p95_ms: percentile(elapsed, 0.95),
                rows_per_second: totalRows / (totalMs / 1000),
                cells_per_second: totalCells / (totalMs / 1000),
            });
        }
    } finally {
        await connection.close();
    }
    fs.mkdirSync(path.dirname(output), { recursive: true });
    fs.writeFileSync(output, JSON.stringify({
        driver: 'node',
        generated_at: Date.now(),
        rows_limit: rowsLimit,
        samples: sampleCount,
        warmup: warmupCount,
        results,
    }, null, 2));
    console.log(`saved ${output}`);
}

main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
