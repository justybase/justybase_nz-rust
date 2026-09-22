const fs = require('node:fs');

const reports = process.argv.slice(2).map((file) => JSON.parse(fs.readFileSync(file, 'utf8')));
const byCase = new Map();
for (const report of reports) {
    for (const item of report.cases) {
        if (!byCase.has(item.name)) byCase.set(item.name, []);
        byCase.get(item.name).push({ driver: report.driver, ...item });
    }
}

console.log('case\tdriver\tavg_ns/op\tp50_ns/op\tp95_ns/op\tresult');
for (const [name, values] of byCase) {
    for (const item of values.sort((a, b) => a.average_ns_per_op - b.average_ns_per_op)) {
        console.log([
            name,
            item.driver,
            item.average_ns_per_op.toFixed(2),
            item.p50_ns_per_op.toFixed(2),
            item.p95_ns_per_op.toFixed(2),
            item.result,
        ].join('\t'));
    }
}
