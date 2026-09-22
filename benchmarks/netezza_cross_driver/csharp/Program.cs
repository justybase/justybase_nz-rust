using System.Diagnostics;
using System.Text.Json;
using JustyBase.NetezzaDriver;

var output = GetArgument("--output") ?? "target/netezza-cross-benchmark/csharp.json";
if (HasArgument("--compat"))
    return await CompatibilityRunner.RunAsync(
        GetArgument("--manifest")
            ?? throw new InvalidOperationException("--manifest is required with --compat"),
        output);
var rowsLimit = NumberEnv("NZ_BENCH_ROWS", 10000);
var sampleCount = NumberEnv("NZ_BENCH_SAMPLES", 5);
var warmupCount = NumberEnv("NZ_BENCH_WARMUP", 1);
var textRepetitions = NumberEnv("NZ_BENCH_TEXT_REPETITIONS", 100);
var database = Environment.GetEnvironmentVariable("NZ_DEV_DB")
    ?? Environment.GetEnvironmentVariable("NZ_DEV_DATABASE")
    ?? "JUST_DATA";
var sourceTable = Environment.GetEnvironmentVariable("NZ_BENCH_SOURCE_TABLE")
    ?? $"{database}.ADMIN.FACTPRODUCTINVENTORY";
var scenariosPath = Environment.GetEnvironmentVariable("NZ_BENCH_SCENARIOS")
    ?? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../../benchmarks/netezza_cross_driver/scenarios.json"));
var scenarios = JsonSerializer.Deserialize<List<Scenario>>(File.ReadAllText(scenariosPath))
    ?? throw new InvalidOperationException("No benchmark scenarios found");

var config = new NzConnection(
    Environment.GetEnvironmentVariable("NZ_DEV_USER") ?? "admin",
    Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("NZ_DEV_PASSWORD is required"),
    Environment.GetEnvironmentVariable("NZ_DEV_HOST") ?? "127.0.0.1",
    database,
    int.TryParse(Environment.GetEnvironmentVariable("NZ_DEV_PORT"), out var port) ? port : 5480);
config.Open();
config.UseStringPool = !string.Equals(
    Environment.GetEnvironmentVariable("NZ_BENCH_CSHARP_STRING_POOL"),
    "0",
    StringComparison.Ordinal);
try
{
    var results = new List<object>();
    foreach (var original in scenarios)
    {
        var scenario = original with
        {
            query = original.query.Replace("__SOURCE_TABLE__", sourceTable).Replace("__ROW_LIMIT__", rowsLimit.ToString()),
            repetitions = original.name == "text-typed-loose" ? textRepetitions : original.repetitions,
        };
        for (var i = 0; i < warmupCount; i++) Consume(config, scenario.query);
        var samples = new List<Sample>();
        for (var sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++)
        {
            var beforeAlloc = GC.GetAllocatedBytesForCurrentThread();
            var stopwatch = Stopwatch.StartNew();
            long rows = 0;
            long cells = 0;
            for (var repetition = 0; repetition < scenario.repetitions; repetition++)
            {
                var result = Consume(config, scenario.query);
                rows += result.rows;
                cells += result.cells;
            }
            stopwatch.Stop();
            samples.Add(new Sample(stopwatch.Elapsed.TotalMilliseconds, rows, cells,
                GC.GetAllocatedBytesForCurrentThread() - beforeAlloc));
        }
        var elapsed = samples.Select(sample => sample.elapsed_ms).ToArray();
        var totalMs = elapsed.Sum();
        var totalRows = samples.Sum(sample => sample.rows);
        var totalCells = samples.Sum(sample => sample.cells);
        results.Add(new
        {
            name = scenario.name,
            description = scenario.description,
            repetitions = scenario.repetitions,
            samples,
            average_ms = totalMs / samples.Count,
            p50_ms = Percentile(elapsed, 0.50),
            p95_ms = Percentile(elapsed, 0.95),
            rows_per_second = totalRows / (totalMs / 1000.0),
            cells_per_second = totalCells / (totalMs / 1000.0),
        });
    }
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(output))!);
    File.WriteAllText(output, JsonSerializer.Serialize(new
    {
        driver = "csharp",
        generated_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        rows_limit = rowsLimit,
        samples = sampleCount,
        warmup = warmupCount,
        results,
    }, new JsonSerializerOptions { WriteIndented = true }));
    Console.WriteLine($"saved {output}");
}
finally
{
    config.Close();
}

return 0;

static (long rows, long cells) Consume(NzConnection connection, string query)
{
    using var command = connection.CreateCommand(query);
    using var reader = command.ExecuteReader();
    long rows = 0;
    long cells = 0;
    while (reader.Read())
    {
        rows++;
        cells += reader.FieldCount;
        for (var index = 0; index < reader.FieldCount; index++)
            _ = reader.GetValue(index);
    }
    return (rows, cells);
}

static int NumberEnv(string name, int fallback) =>
    int.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value > 0 ? value : fallback;

static double Percentile(double[] values, double percentile)
{
    Array.Sort(values);
    return values[(int)Math.Round((values.Length - 1) * percentile)];
}

static string? GetArgument(string name)
{
    var args = Environment.GetCommandLineArgs();
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
}

static bool HasArgument(string name) =>
    Environment.GetCommandLineArgs().Any(argument => argument == name);

record Scenario(string name, string description, string query, int repetitions);
record Sample(double elapsed_ms, long rows, long cells, long allocated_bytes);
