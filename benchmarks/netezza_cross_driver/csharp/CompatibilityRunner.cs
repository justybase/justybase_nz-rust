using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using JustyBase.NetezzaDriver;

/// Executes the shared Rust/C# compatibility manifest without involving the
/// Node driver. The output is deliberately value-oriented: native CLR types
/// are mapped to stable categories before the Rust comparator sees them.
internal static class CompatibilityRunner
{
    public static Task<int> RunAsync(string manifestPath, string output)
    {
        var manifest = JsonSerializer.Deserialize<CompatibilityManifest>(
            File.ReadAllText(manifestPath), JsonOptions())
            ?? throw new InvalidOperationException("Compatibility manifest is empty.");

        var host = Required("NZ_DEV_HOST");
        var user = Required("NZ_DEV_USER");
        var password = Required("NZ_DEV_PASSWORD");
        var database = Environment.GetEnvironmentVariable("NZ_DEV_DB")
            ?? Environment.GetEnvironmentVariable("NZ_DEV_DATABASE")
            ?? "JUST_DATA";
        var port = int.TryParse(Environment.GetEnvironmentVariable("NZ_DEV_PORT"), out var parsedPort)
            ? parsedPort
            : 5480;

        using var connection = new NzConnection(user, password, host, database, port);
        connection.Open(ClientTypeId.SqlDotnet);
        var results = new List<CompatibilityResult>(manifest.Cases.Count);

        foreach (var testCase in manifest.Cases)
        {
            var sql = Expand(testCase.Sql, database);
            CompatibilityResult result;
            try
            {
                foreach (var setup in testCase.Setup ?? [])
                    Execute(connection, Expand(setup, database));

                result = testCase.Mode.Equals("execute", StringComparison.OrdinalIgnoreCase)
                    ? RunExecute(connection, sql)
                    : RunQuery(connection, sql);
            }
            catch (Exception exception)
            {
                result = CompatibilityResult.Failed(NormalizeError(exception));
            }
            finally
            {
                foreach (var cleanup in testCase.Cleanup ?? [])
                {
                    try
                    {
                        Execute(connection, Expand(cleanup, database));
                    }
                    catch
                    {
                        // Cleanup must not hide the result of the actual case.
                    }
                }
            }

            results.Add(result with { Id = testCase.Id });
        }

        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(output))!);
        File.WriteAllText(output, JsonSerializer.Serialize(
            new CompatibilityReport("csharp", manifest.Version, results), JsonOptions()));
        Console.WriteLine($"saved {output} ({results.Count} compatibility cases)");
        return Task.FromResult(0);
    }

    private static CompatibilityResult RunExecute(NzConnection connection, string sql)
    {
        using var command = connection.CreateCommand(sql);
        return new CompatibilityResult(null, [], [], [], command.ExecuteNonQuery(), null);
    }

    private static CompatibilityResult RunQuery(NzConnection connection, string sql)
    {
        using var command = connection.CreateCommand(sql);
        using var reader = command.ExecuteReader();
        var resultSets = new List<CompatibilityResultSet>();
        do
        {
            var columns = Enumerable.Range(0, reader.FieldCount)
                .Select(reader.GetName)
                .ToArray();
            var rows = new List<CompatibilityCell?[]>();
            while (reader.Read())
            {
                var row = new CompatibilityCell?[reader.FieldCount];
                for (var index = 0; index < reader.FieldCount; index++)
                {
                    row[index] = reader.IsDBNull(index)
                        ? null
                        : Encode(reader.GetValue(index));
                }
                rows.Add(row);
            }
            resultSets.Add(new CompatibilityResultSet(columns, rows));
        } while (reader.NextResult());

        return new CompatibilityResult(null, resultSets, [], [], null, null);
    }

    private static void Execute(NzConnection connection, string sql)
    {
        using var command = connection.CreateCommand(sql);
        command.ExecuteNonQuery();
    }

    private static CompatibilityCell Encode(object value)
    {
        if (value is bool boolean)
            return new CompatibilityCell("bool", boolean ? "true" : "false");
        if (value is byte[] bytes)
            return new CompatibilityCell("bytes", Convert.ToHexString(bytes));
        if (value is DateTime dateTime)
            return new CompatibilityCell(
                "datetime", dateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture));
        if (value is DateTimeOffset dateTimeOffset)
            return new CompatibilityCell(
                "datetimeoffset", dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz", CultureInfo.InvariantCulture));
        if (value is TimeSpan timeSpan)
            return new CompatibilityCell("timespan", timeSpan.ToString("c", CultureInfo.InvariantCulture));
        if (value is float single)
            return new CompatibilityCell("float", single.ToString("R", CultureInfo.InvariantCulture));
        if (value is double doubleValue)
            return new CompatibilityCell("float", doubleValue.ToString("R", CultureInfo.InvariantCulture));
        if (value is decimal decimalValue)
            return new CompatibilityCell("numeric", decimalValue.ToString(CultureInfo.InvariantCulture));
        if (value is sbyte or byte or short or ushort or int or uint or long or ulong)
            return new CompatibilityCell("integer", Convert.ToString(value, CultureInfo.InvariantCulture)!);
        return new CompatibilityCell("text", value.ToString() ?? string.Empty);
    }

    private static string NormalizeError(Exception exception) => exception switch
    {
        NetezzaException databaseException => $"database:{databaseException.GetType().Name}",
        _ => $"{exception.GetType().Name}",
    };

    private static string Expand(string sql, string database) =>
        sql.Replace("__DB__", database, StringComparison.OrdinalIgnoreCase);

    private static string Required(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"Environment variable {name} is required.");

    private static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = true,
    };

    private sealed class CompatibilityManifest
    {
        public int Version { get; set; }
        public List<CompatibilityCase> Cases { get; set; } = [];
    }

    private sealed class CompatibilityCase
    {
        public string Id { get; set; } = "";
        public string Category { get; set; } = "";
        public string Mode { get; set; } = "query";
        public string Sql { get; set; } = "";
        public List<string>? Setup { get; set; }
        public List<string>? Cleanup { get; set; }
        public bool CompareTypes { get; set; }
    }

    private sealed record CompatibilityReport(
        string Driver,
        int ManifestVersion,
        List<CompatibilityResult> Cases);

    private sealed record CompatibilityResult(
        string? Id,
        List<CompatibilityResultSet> ResultSets,
        List<string> Columns,
        List<CompatibilityCell?[]> Rows,
        int? Affected,
        string? Error)
    {
        public static CompatibilityResult Failed(string error) =>
            new(null, [], [], [], null, error);
    }

    private sealed record CompatibilityResultSet(
        string[] Columns,
        List<CompatibilityCell?[]> Rows);

    private sealed record CompatibilityCell(string Type, string Value);
}
