const fs = require('node:fs');
const path = require('node:path');

const driverRoot = process.env.NZ_NODE_DRIVER_ROOT || path.resolve(__dirname, '../../../justybase_netezza_node_driver');
const { getCsNumeric } = require(path.join(driverRoot, 'dist/cjs/types/TypeConversions.js'));
const casesPath = process.env.NZ_BENCH_NUMERIC_CASES || path.join(__dirname, 'numeric_cases.json');
const output = process.argv[2] || 'target/netezza-cross-benchmark/node-numeric-replay.json';
const iterations = Number(process.env.NZ_BENCH_NUMERIC_ITERATIONS || 200000);
const warmup = Number(process.env.NZ_BENCH_NUMERIC_WARMUP || 10000);
const samplesCount = Number(process.env.NZ_BENCH_NUMERIC_SAMPLES || 5);
const cases = JSON.parse(fs.readFileSync(casesPath, 'utf8'));

function encodeNumeric(value, scale, partCount) {
    const negative = value.startsWith('-');
    const unsigned = negative ? value.slice(1) : value;
    const [integer, fraction = ''] = unsigned.split('.');
    const digits = `${integer}${fraction.slice(0, scale).padEnd(scale, '0')}`;
    let raw = BigInt(digits || '0');
    if (negative) raw = BigInt.asUintN(partCount * 32, -raw);
    const data = Buffer.alloc(partCount * 4);
    for (let index = partCount - 1; index >= 0; index--) {
        data.writeUInt32LE(Number(raw & 0xffffffffn), index * 4);
        raw >>= 32n;
    }
    return data;
}

function checksum(value) {
    return typeof value === 'string' ? value.length : 1;
}

function percentile(values, p) {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted[Math.round((sorted.length - 1) * p)];
}

let globalChecksum = 0;
const reportCases = [];
for (const item of cases) {
    const data = encodeNumeric(item.value, item.scale, item.digit_count);
    const expected = getCsNumeric(data, item.precision, item.scale, item.digit_count);
    const result = typeof expected === 'string' ? expected : String(expected);
    for (let i = 0; i < warmup; i++) {
        globalChecksum ^= checksum(getCsNumeric(data, item.precision, item.scale, item.digit_count));
    }
    const samples = [];
    for (let sample = 0; sample < samplesCount; sample++) {
        const start = process.hrtime.bigint();
        for (let i = 0; i < iterations; i++) {
            globalChecksum ^= checksum(getCsNumeric(data, item.precision, item.scale, item.digit_count));
        }
        samples.push(Number(process.hrtime.bigint() - start) / iterations);
    }
    reportCases.push({
        name: item.name,
        precision: item.precision,
        scale: item.scale,
        digit_count: item.digit_count,
        result,
        average_ns_per_op: samples.reduce((sum, value) => sum + value, 0) / samples.length,
        p50_ns_per_op: percentile(samples, 0.50),
        p95_ns_per_op: percentile(samples, 0.95),
    });
}

fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, JSON.stringify({
    driver: 'node',
    iterations,
    warmup,
    cases: reportCases,
}, null, 2));
console.log(`saved ${output} (checksum=${globalChecksum})`);
