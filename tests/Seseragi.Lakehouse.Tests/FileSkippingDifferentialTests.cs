using System.Globalization;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Delta.Skipping;
using Kaminari.Seseragi.Lakehouse.Skipping;
using P = Kaminari.Seseragi.Lakehouse.Skipping.Predicate;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// File skipping against the rows themselves: random files (rows, and their statistics as Delta writers write them)
/// and random predicates; a file holding a matching row is never left out. Predicates are evaluated by the IR's
/// semantics (Kleene logic, a comparison with null is null, NaN greater than every number and equal to itself).
/// </summary>
public class FileSkippingDifferentialTests
{
    private static readonly StructType s_schema = StructType.Parse("""
        {"type":"struct","fields":[
          {"name":"i","type":"long","nullable":true,"metadata":{}},
          {"name":"s","type":"string","nullable":true,"metadata":{}},
          {"name":"d","type":"double","nullable":true,"metadata":{}},
          {"name":"dt","type":"date","nullable":true,"metadata":{}},
          {"name":"ts","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"dec","type":"decimal(10,2)","nullable":true,"metadata":{}},
          {"name":"sm","type":"decimal(5,2)","nullable":true,"metadata":{}},
          {"name":"big","type":"decimal(38,4)","nullable":true,"metadata":{}},
          {"name":"b","type":"boolean","nullable":true,"metadata":{}},
          {"name":"part","type":"string","nullable":true,"metadata":{}},
          {"name":"pd","type":"date","nullable":true,"metadata":{}},
          {"name":"pb","type":"boolean","nullable":true,"metadata":{}},
          {"name":"pdec","type":"decimal(10,2)","nullable":true,"metadata":{}},
          {"name":"pts","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"n","type":{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}
        ]}
        """);

    private static readonly string[] s_partitions = ["part", "pd", "pb", "pdec", "pts"];
    private static readonly string[] s_columns = ["i", "s", "d", "dt", "ts", "dec", "sm", "big", "b", "part", "pd", "pb", "pdec", "pts", "n.x"];
    private static readonly DateTimeOffset s_t0 = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly string[] s_strings = ["", "a", "ab", "b", "ba", "z"];
    private static readonly double[] s_doubles = [-1.5, 0.0, 0.5, 2.0, 7.25];
    private static readonly long[] s_micros = [0, 500, 999, 1000, 1500, 2_000_999];
    private static readonly decimal[] s_decimals = [-1.25m, 0m, 2.50m, 3.75m];
    private static readonly string[] s_parts = ["east", "west"];
    private static readonly decimal[] s_bigs = [-100000000000000000000m, 0m, 1234567890123456789012.3456m, 5.0001m];

    private static object? Value(Random random, string column, bool nans) => random.Next(5) == 0 ? null : column switch
    {
        "i" => (long)random.Next(-5, 6),
        "s" => s_strings[random.Next(s_strings.Length)],
        "d" => nans && random.Next(6) == 0 ? double.NaN : s_doubles[random.Next(s_doubles.Length)],
        "dt" => DateOnly.FromDateTime(s_t0.UtcDateTime).AddDays(random.Next(6)),
        "ts" => s_t0.AddTicks(10 * s_micros[random.Next(s_micros.Length)]),
        "dec" => s_decimals[random.Next(s_decimals.Length)],
        "sm" => s_decimals[random.Next(s_decimals.Length)],
        "big" => s_bigs[random.Next(s_bigs.Length)],
        "b" or "pb" => random.Next(2) == 0,
        "part" => s_parts[random.Next(s_parts.Length)],
        "pd" => DateOnly.FromDateTime(s_t0.UtcDateTime).AddDays(random.Next(3)),
        "pdec" => s_decimals[random.Next(s_decimals.Length)],
        "pts" => s_t0.AddTicks(10 * s_micros[random.Next(s_micros.Length)]),
        "n.x" => random.Next(0, 5),
        _ => throw new ArgumentException(column),
    };

    /// <summary>A file of rows and its add action, with statistics as Spark writes them (or, with <paramref name="arrowNaN"/>, minimum and maximum ignoring NaN as Arrow writers do).</summary>
    private static (Dictionary<string, object?>[] Rows, AddAction File) MakeFile(Random random, bool nans, bool arrowNaN)
    {
        int count = random.Next(0, 6);
        // One value per partition column for the whole file, null now and then.
        Dictionary<string, object?> parts = s_partitions.ToDictionary(c => c, c => random.Next(4) == 0 ? null : NonNull(random, c));
        var rows = new Dictionary<string, object?>[count];
        for (int r = 0; r < count; r++)
        {
            rows[r] = s_columns.ToDictionary(c => c, c => parts.TryGetValue(c, out object? p) ? p : Value(random, c, nans));
        }
        string? stats = random.Next(10) == 0 ? null : Stats(rows, random, arrowNaN);
        var file = new AddAction
        {
            Path = "f.parquet",
            Stats = stats,
            PartitionValues = parts.ToDictionary(p => p.Key, p => PartitionText(p.Value)),
        };
        return (rows, file);
    }

    private static object NonNull(Random random, string column)
    {
        object? value = null;
        while (value is null) value = Value(random, column, nans: false);
        return value;
    }

    /// <summary>A partition value as the log writes it.</summary>
    private static string? PartitionText(object? value) => value switch
    {
        null => null,
        bool b => b ? "true" : "false",
        DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        decimal m => m.ToString(CultureInfo.InvariantCulture),
        DateTimeOffset t => t.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        _ => (string)value,
    };

    private static string Stats(Dictionary<string, object?>[] rows, Random random, bool arrowNaN)
    {
        var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            w.WriteNumber("numRecords", rows.Length);
            foreach (string kind in (string[])["minValues", "maxValues", "nullCount"])
            {
                w.WriteStartObject(kind);
                bool nestedOpen = false;
                foreach (string column in s_columns.Where(c => !s_partitions.Contains(c)))
                {
                    if (random.Next(8) == 0) continue; // a column without statistics
                    if (column == "n.x")
                    {
                        w.WriteStartObject("n");
                        nestedOpen = true;
                    }
                    string name = column == "n.x" ? "x" : column;
                    object?[] values = [.. rows.Select(r => r[column]).Where(v => v is not null)];
                    if (kind == "nullCount")
                    {
                        w.WriteNumber(name, rows.Length - values.Length);
                    }
                    else if (values.Length > 0 && column != "b") // booleans get no bounds
                    {
                        bool max = kind == "maxValues";
                        if (column == "d" && arrowNaN) values = [.. values.Where(v => !double.IsNaN((double)v!))];
                        if (values.Length > 0) WriteBound(w, name, column, max ? values.Max(Comparer)! : values.Min(Comparer)!);
                    }
                    if (nestedOpen)
                    {
                        w.WriteEndObject();
                        nestedOpen = false;
                    }
                }
                w.WriteEndObject();
            }
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static void WriteBound(Utf8JsonWriter w, string name, string column, object value)
    {
        switch (value)
        {
            case long l: w.WriteNumber(name, l); break;
            case int n: w.WriteNumber(name, n); break;
            case double d when double.IsNaN(d): w.WriteString(name, "NaN"); break; // Spark: the maximum with NaN is NaN
            case double d: w.WriteNumber(name, d); break;
            case decimal m: w.WriteNumber(name, m); break;
            case string s: w.WriteString(name, s); break;
            case DateOnly dt: w.WriteString(name, dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)); break;
            case DateTimeOffset ts:
                // Cut to milliseconds, as Spark writes timestamps in statistics.
                var cut = new DateTimeOffset(ts.UtcTicks - ts.UtcTicks % TimeSpan.TicksPerMillisecond, TimeSpan.Zero);
                w.WriteString(name, cut.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture));
                break;
            default: throw new ArgumentException(column);
        }
    }

    private static readonly Comparer<object?> Comparer = Comparer<object?>.Create(Compare);

    /// <summary>The IR's order of two non-null values of one column (NaN greatest, equal to itself; strings ordinal).</summary>
    private static int Compare(object? a, object? b)
    {
        if (a is double x && b is double y)
            return double.IsNaN(x) || double.IsNaN(y) ? (double.IsNaN(x) ? 1 : 0) - (double.IsNaN(y) ? 1 : 0) : x.CompareTo(y);
        if (a is string s && b is string t) return string.CompareOrdinal(s, t);
        return ((IComparable)a!).CompareTo(b);
    }

    // ------------------------------------------------------------------ predicates and their value on a row

    private static P RandomPredicate(Random random, int depth, bool withDoubles)
    {
        int pick = depth == 0 ? random.Next(2) : random.Next(5);
        switch (pick)
        {
            case 0:
            case 1:
            {
                string[] columns = withDoubles ? s_columns : [.. s_columns.Where(c => c != "d")];
                string column = columns[random.Next(columns.Length)];
                ColumnPath path = column == "n.x" ? new ColumnPath("n", "x") : new ColumnPath(column);
                if (pick == 1 && random.Next(3) == 0) return random.Next(2) == 0 ? P.IsNull(path) : P.IsNotNull(path);
                object literal = NonNull(random, column);
                if (column == "i" && random.Next(4) == 0) literal = (long)random.Next(-8, 9);
                if (literal is DateTimeOffset ts && random.Next(2) == 0) literal = ts.AddTicks(10);
                return P.Compare(path, (CompareOp)random.Next(6), literal);
            }
            case 2:
                return P.And([.. Enumerable.Range(0, random.Next(1, 4)).Select(_ => RandomPredicate(random, depth - 1, withDoubles))]);
            case 3:
                return P.Or([.. Enumerable.Range(0, random.Next(1, 4)).Select(_ => RandomPredicate(random, depth - 1, withDoubles))]);
            default:
                return P.Not(RandomPredicate(random, depth - 1, withDoubles));
        }
    }

    /// <summary>The predicate's value on a row: true, false or null (Kleene).</summary>
    private static bool? Eval(P predicate, Dictionary<string, object?> row) => predicate switch
    {
        ComparePredicate c => row[c.Column.ToString()] is { } v ? Holds(c.Op, Compare(v, c.Value)) : null,
        NullPredicate n => (row[n.Column.ToString()] is null) == n.WantNull,
        JunctionPredicate { IsAnd: true } j => j.Children.Select(c => Eval(c, row)).Aggregate((bool?)true, (a, b) =>
            a == false || b == false ? false : a is null || b is null ? null : true),
        JunctionPredicate j => j.Children.Select(c => Eval(c, row)).Aggregate((bool?)false, (a, b) =>
            a == true || b == true ? true : a is null || b is null ? null : false),
        NotPredicate n => Eval(n.Child, row) is bool b ? !b : null,
        _ => throw new ArgumentException(predicate.ToString()),
    };

    private static bool Holds(CompareOp op, int c) => op switch
    {
        CompareOp.Eq => c == 0,
        CompareOp.Ne => c != 0,
        CompareOp.Lt => c < 0,
        CompareOp.Le => c <= 0,
        CompareOp.Gt => c > 0,
        _ => c >= 0,
    };

    [Theory]
    [InlineData(1, false, false)]
    [InlineData(2, true, false)]
    [InlineData(3, true, true)]
    public void AFileWithAMatchingRow_IsNeverLeftOut(int seed, bool nans, bool arrowNaN)
    {
        var random = new Random(seed);
        int checks = 0, skipped = 0;
        for (int round = 0; round < 3000; round++)
        {
            P predicate = RandomPredicate(random, random.Next(4), withDoubles: true);
            var filter = new FileFilter(s_schema, s_partitions, predicate);
            for (int f = 0; f < 4; f++)
            {
                var (rows, file) = MakeFile(random, nans, arrowNaN);
                bool matches = rows.Any(r => Eval(predicate, r) == true);
                bool kept = filter.MightMatch(file);
                Assert.True(kept || !matches, $"{predicate} left out a file with a matching row; stats {file.Stats}, partition {file.PartitionValues["part"]}");
                checks++;
                if (!kept) skipped++;
            }
        }
        Assert.True(skipped > checks / 10, $"only {skipped} of {checks} files left out");
    }

    [Theory]
    [InlineData(11, false)]
    [InlineData(12, true)]
    public void Statistics_ReadAsTheJsonDocumentOracleReadsThem(int seed, bool arrowNaN)
    {
        // The statistics read token by token (from a string, or from UTF-8 as a commit's reader keeps them) against
        // the reader they replaced (a JsonDocument per file): the same decision for every file, its statistics valid
        // or mutated (repeated sections and fields, values of other kinds, escaped names, broken text, deep nesting),
        // file by file and for a whole list at once (judged on several threads).
        var random = new Random(seed);
        List<AddAction> files = [];
        for (int f = 0; f < 6000; f++)
        {
            var (_, file) = MakeFile(random, nans: true, arrowNaN);
            string? stats = file.Stats is null || random.Next(3) == 0 ? file.Stats : MutateStats(random, file.Stats);
            if (random.Next(40) == 0) stats = null;
            files.Add(random.Next(2) == 0
                ? new AddAction { Path = file.Path, PartitionValues = file.PartitionValues, Stats = stats }
                : new AddAction { Path = file.Path, PartitionValues = file.PartitionValues, StatsText = stats is null ? null : Encoding.UTF8.GetBytes(stats) });
        }
        int skipped = 0;
        for (int round = 0; round < 300; round++)
        {
            P predicate = RandomPredicate(random, random.Next(4), withDoubles: true);
            var filter = new FileFilter(s_schema, s_partitions, predicate);
            JsonDocumentFileSkipping? oracle = JsonDocumentFileSkipping.Create(s_schema, s_partitions, FileFilter.ToFilter(predicate, s_schema));
            List<AddAction> expected = [.. files.Where(f => oracle?.MightMatch(f) ?? true)];
            for (int i = 0; i < files.Count; i += 7)
                Assert.True(filter.MightMatch(files[i]) == (oracle?.MightMatch(files[i]) ?? true), $"{predicate}; stats {files[i].Stats}");
            List<AddAction> matching = filter.Matching(files);
            Assert.True(expected.SequenceEqual(matching), $"{predicate}: {matching.Count} files kept, the oracle keeps {expected.Count}");
            skipped += files.Count - matching.Count;
        }
        Assert.True(skipped > 0);
    }

    private static string MutateStats(Random random, string stats)
    {
        switch (random.Next(11))
        {
            case 0:
            {
                // A few bytes changed: mostly broken text.
                var text = new StringBuilder(stats);
                for (int m = random.Next(1, 3); m > 0 && text.Length > 0; m--)
                {
                    int at = random.Next(text.Length);
                    const string Chars = "{}[]:,\"\\ 0-.x";
                    char c = Chars[random.Next(Chars.Length)];
                    if (random.Next(2) == 0) text[at] = c;
                    else text.Insert(at, c);
                }
                return text.ToString();
            }
            case 1: return stats.Insert(1, "\"minValues\":{},\"numRecords\":\"x\",");          // earlier copies, overridden
            case 2: return stats[..^1] + ",\"minValues\":{\"i\":3,\"s\":null}}";               // a later section counts as a whole
            case 3: return stats[..^1] + ",\"maxValues\":5,\"nullCount\":[1]}";                // sections that are not objects
            case 4: return random.Next(2) == 0
                ? stats.Replace("\"numRecords\":", "\"numRecords\":1.5,\"numRecords\":")
                : stats[..^1] + ",\"numRecords\":-1}";                                                 // the last count, unusable, counts
            case 5: return stats.Replace("\"i\":", "\"\\u0069\":").Replace("\"ts\":", "\"t\\u0073\":");
            case 6: return stats.Replace("\"n\":{", "\"n\":{\"x\":7},\"n\":{").Replace("\"s\":", "\"s\":{\"y\":1},\"s\":");
            case 7: return stats.Replace("\"i\":", "\"i\":true,\"i\":").Replace("\"d\":", "\"d\":\"1\",\"x\":");
            case 8: return stats[..^1] + ",\"z\":" + new string('[', random.Next(60, 66)) + new string(']', 0) + "}"; // broken
            case 9:
            {
                int depth = random.Next(60, 66); // the object itself is level 1
                return stats[..^1] + ",\"z\":" + new string('[', depth - 1) + new string(']', depth - 1) + "}";
            }
            default: return stats + (random.Next(2) == 0 ? " \n" : " x");
        }
    }
}
