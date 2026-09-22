const fs = require('node:fs');

const files = process.argv.slice(2);
if (!files.length) {
    console.error('usage: node report.js csharp.json node.json rust.json');
    process.exit(2);
}

const reports = files.map((file) => JSON.parse(fs.readFileSync(file, 'utf8')));
const byScenario = new Map();
for (const report of reports) {
    for (const result of report.results) {
        if (!byScenario.has(result.name)) byScenario.set(result.name, []);
        byScenario.get(result.name).push({ driver: report.driver, ...result });
    }
}

console.log('scenario\tdriver\tavg_ms\tp50_ms\tp95_ms\trows/s\tcells/s');
for (const [scenario, results] of byScenario) {
    for (const result of results.sort((a, b) => a.average_ms - b.average_ms)) {
        console.log([
            scenario,
            result.driver,
            result.average_ms.toFixed(2),
            result.p50_ms.toFixed(2),
            result.p95_ms.toFixed(2),
            result.rows_per_second.toFixed(0),
            result.cells_per_second.toFixed(0),
        ].join('\t'));
    }
}
