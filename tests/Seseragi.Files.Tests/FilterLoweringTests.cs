using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Filters;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Seseragi.Files.Tests;

/// <summary>
/// Samidare's <c>FilterLowering</c> against <see cref="IrEvaluator"/>: random IR filters over columns of every type the
/// readers produce, with literals of other kinds, finer units, between stored values and outside the columns' ranges.
/// An exact lowering selects exactly the rows the filter keeps; one with dropped conjuncts at least those.
/// </summary>
public class FilterLoweringTests
{
    /// <summary>
    /// A column: its type, model values (the CLR values IR literals hold), their stored form, literals of its domain
    /// (lowered exactly) and literals of other kinds the evaluator still compares (lowering drops their conjunct).
    /// </summary>
    private sealed record Spec(string Name, IArrowType Type, object[] Pool, Func<object, object> Store, object[] Literals, object[] Foreign);

    private static readonly long EpochTicks = DateTime.UnixEpoch.Ticks;
    private static readonly int EpochDay = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;

    private static long Units(long ticks, TimeUnit unit) => unit switch
    {
        TimeUnit.Second => ticks / TimeSpan.TicksPerSecond,
        TimeUnit.Millisecond => ticks / TimeSpan.TicksPerMillisecond,
        TimeUnit.Microsecond => ticks / 10,
        _ => ticks * 100,
    };

    private static object Same(object v) => v;

    private static object[] Of<T>(params T[] values) => [.. values.Select(v => (object)v!)];

    private static readonly DateTime T0 = new(2024, 3, 1, 12, 30, 15, DateTimeKind.Unspecified);
    private static readonly DateTimeOffset O0 = new(2024, 3, 1, 12, 30, 15, TimeSpan.Zero);

    private static readonly object[] Floats = Of(double.NaN, double.NegativeInfinity, -1.5, -0.0, 0.0, 1.0, 2.5, double.PositiveInfinity);
    private static readonly object[] Strings = Of("", "a", "ab", "abc", "Banana", "apple", "äpfel", "abcdefghijkl", "abcdefghijklm", "abcdXXXXXXXXXXXXXXXX");

    private static readonly Spec[] Specs =
    [
        new("i8", Int8Type.Default, Of<sbyte>(sbyte.MinValue, -3, 0, 1, 2, 5, sbyte.MaxValue), v => (long)(sbyte)v,
            Of<object>(-3, 0, 2, 5, 1000, -1000, 127, 2.5, -0.5, 2.5m, 3m, long.MaxValue, ulong.MaxValue, (sbyte)1, (short)-128),
            Of<object>(double.NaN, 1e40)),
        new("u8", UInt8Type.Default, Of<byte>(0, 1, 2, 254, byte.MaxValue), v => (long)(byte)v,
            Of<object>(-1, 0, 2, 254, 255, 256, 254.5, 255.5, (byte)1), []),
        new("u16", UInt16Type.Default, Of<ushort>(0, 1, 2, 40000, ushort.MaxValue), v => (long)(ushort)v,
            Of<object>(-1, 0, 2, 40000, 70000, 1.5, 65535.5, (ushort)1, 2m), Of<object>(double.PositiveInfinity)),
        new("i32", Int32Type.Default, Of(int.MinValue, -3, 0, 1, 2, 70000, int.MaxValue), v => (long)(int)v,
            Of<object>(-3, 0, 2, 70000, int.MaxValue, 3_000_000_000L, -2.5, 0.5, 69999.9, 2m), Of<object>(1e40)),
        // Beyond 2^53 a double rounds: 64-bit integers take no floating literal (the engine compares those as doubles).
        new("i64", Int64Type.Default, Of(long.MinValue, -3L, 0L, 1L, 2L, 1L << 40, (1L << 53) + 1, long.MaxValue), v => (long)v,
            Of<object>(long.MinValue, -3L, 2L, (1L << 53) + 1, long.MaxValue, ulong.MaxValue, 9_223_372_036_854_775_808m, -9_223_372_036_854_775_809m, 2.5m),
            Of<object>(double.NaN, 2.5, -0.25, 9_007_199_254_740_992.0)),
        new("u64", UInt64Type.Default, Of(0UL, 1UL, 2UL, (1UL << 53) + 1, 1UL << 63, ulong.MaxValue), v => (ulong)v,
            Of<object>(0UL, 2UL, 1UL << 63, ulong.MaxValue, -1L, 18_446_744_073_709_551_616m), Of<object>(1e40, 1.5, 9_007_199_254_740_992.0)),
        new("f64", DoubleType.Default, Floats, Same,
            Of<object>(double.NaN, -1.5, -0.0, 0.0, 2.5, double.PositiveInfinity, 0.1f, 2, 2.5m, (Half)1, 9_007_199_254_740_992L),
            Of<object>(9_007_199_254_740_993L, 0.30000000000000004m)), // no double is exactly either
        new("f32", FloatType.Default, Floats, v => (float)(double)v,
            Of<object>(double.NaN, -1.5, 0.0, 2.5, 0.1, -0.1, 1e300, -1e300, 0.1f, 2, 2.5m, 0.1m, double.NegativeInfinity), []),
        new("f16", HalfFloatType.Default, Floats, v => (Half)(double)v,
            Of<object>(double.NaN, -1.5, 0.0, 2.5, 0.1, 70000.0, -70000.0, (Half)2.5, 3, double.PositiveInfinity), []),
        new("dec", new Decimal128Type(10, 2), Of(-1.00m, 0m, 0.01m, 1.50m, 2.25m, 12345678.99m), v => Int128.Parse(((decimal)v * 100).ToString("0")),
            Of<object>(1.5m, 1.505m, 0.001m, -0.005m, 2, 0, long.MaxValue, 12345678.99m, 99999999.999m), Of<object>(1.5, 0.1)),
        new("flag", BooleanType.Default, Of(true, false), Same, Of<object>(true, false), []),
        new("utf8", StringType.Default, Strings, Same, Of<object>([.. Strings, "b", "abcdXXXXXXXXXXXXXXXY", "zz"]), []),
        new("view", StringViewType.Default, Strings, Same, Of<object>([.. Strings, "b", "abcdefghijklmn"]), []),
        new("dict", new DictionaryType(Int32Type.Default, StringType.Default, false), Strings, Same, Of<object>([.. Strings, "b"]), []),
        new("date32", Date32Type.Default, Of(new DateOnly(1969, 12, 31), new DateOnly(1970, 1, 1), new DateOnly(2024, 2, 29)), v => (long)(((DateOnly)v).DayNumber - EpochDay),
            Of<object>(new DateOnly(1970, 1, 1), new DateOnly(2024, 2, 29), new DateOnly(2024, 3, 1), DateOnly.MaxValue), []),
        new("date64", Date64Type.Default, Of(new DateOnly(1969, 12, 31), new DateOnly(1970, 1, 1), new DateOnly(2024, 2, 29)),
            v => (long)(((DateOnly)v).DayNumber - EpochDay) * 86_400_000L,
            Of<object>(new DateOnly(1970, 1, 1), new DateOnly(2024, 2, 29), DateOnly.MinValue), []),
        new("ts_ms", new TimestampType(TimeUnit.Millisecond, (string?)null), Of(T0, T0.AddMilliseconds(1), T0.AddDays(-20000)),
            v => Units(((DateTime)v).Ticks - EpochTicks, TimeUnit.Millisecond),
            Of<object>(T0, T0.AddTicks(1), T0.AddTicks(-1), T0.AddMilliseconds(1), DateTime.MinValue, DateTime.MaxValue), Of<object>(O0)),
        new("ts_ns_utc", new TimestampType(TimeUnit.Nanosecond, "UTC"), Of(O0, O0.AddTicks(1), O0.AddDays(-20000)),
            v => Units(((DateTimeOffset)v).UtcTicks - EpochTicks, TimeUnit.Nanosecond),
            Of<object>(O0, O0.ToOffset(TimeSpan.FromHours(9)), O0.AddTicks(1), O0.AddTicks(2), DateTimeOffset.MaxValue), Of<object>(T0)),
        new("ts_s", new TimestampType(TimeUnit.Second, (string?)null), Of(T0, T0.AddSeconds(1)), v => Units(((DateTime)v).Ticks - EpochTicks, TimeUnit.Second),
            Of<object>(T0, T0.AddMilliseconds(500), T0.AddSeconds(-1).AddTicks(1)), []),
        new("time32", new Time32Type(TimeUnit.Millisecond), Of(new TimeOnly(0, 0), new TimeOnly(12, 30, 15, 250), new TimeOnly(23, 59, 59, 999)),
            v => Units(((TimeOnly)v).Ticks, TimeUnit.Millisecond),
            Of<object>(new TimeOnly(12, 30, 15, 250), new TimeOnly(12, 30, 15, 250).Add(TimeSpan.FromTicks(1)), TimeOnly.MaxValue), []),
        new("time64", new Time64Type(TimeUnit.Microsecond), Of(new TimeOnly(0, 0), new TimeOnly(1, 2, 3, 4, 5)), v => Units(((TimeOnly)v).Ticks, TimeUnit.Microsecond),
            Of<object>(new TimeOnly(1, 2, 3, 4, 5), new TimeOnly(1, 2, 3, 4, 5).Add(TimeSpan.FromTicks(7)), TimeOnly.MinValue), []),
        new("dur", DurationType.Second, Of(TimeSpan.Zero, TimeSpan.FromSeconds(-90), TimeSpan.FromSeconds(3)), v => Units(((TimeSpan)v).Ticks, TimeUnit.Second),
            Of<object>(TimeSpan.Zero, TimeSpan.FromSeconds(1.5), TimeSpan.FromSeconds(-90), TimeSpan.FromTicks(-1), TimeSpan.MaxValue), []),
    ];

    // A struct column: its field "a" is an Int32 spec, "b" a string spec.
    private static readonly StructType Struct = new([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);

    private static LiteralValue Lit(object? v) => v is null ? LiteralValue.NewNull(Kaminari.Samidare.IR.LogicalType.Unknown) : LiteralValue.OfObject(v);

    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);

    private static ScalarExpr Field(string parent, string name) =>
        ScalarExpr.NewFunction(ScalarFunction.StructFieldByName, ListModule.OfSeq([Col(parent), ScalarExpr.NewLiteral(LiteralValue.NewString(name))]),
            Kaminari.Samidare.IR.LogicalType.Unknown);

    private static ScalarExpr Bin(BinaryOp op, ScalarExpr l, ScalarExpr r) => ScalarExpr.NewBinary(op, l, r);

    private static ScalarExpr Not(ScalarExpr e) => ScalarExpr.NewUnary(UnaryOp.Not, e);

    private static readonly BinaryOp[] Comparisons = [BinaryOp.Eq, BinaryOp.NotEq, BinaryOp.Lt, BinaryOp.LtEq, BinaryOp.Gt, BinaryOp.GtEq, BinaryOp.EqMissing, BinaryOp.NotEqMissing];

    // ---------------------------------------------------------------------------------------------------------------
    // Data

    private sealed class Table(Schema schema, RecordBatch batch, Dictionary<string, object?[]> model)
    {
        public Schema Schema { get; } = schema;
        public RecordBatch Batch { get; } = batch;
        public Dictionary<string, object?[]> Model { get; } = model;
        public int Length => Batch.Length;
    }

    private static Table RandomTable(Random random, int length)
    {
        List<Field> fields = [];
        List<IArrowArray> arrays = [];
        Dictionary<string, object?[]> model = [];
        foreach (Spec spec in Specs)
        {
            object?[] values = [.. Enumerable.Range(0, length).Select(_ => random.Next(5) == 0 ? null : spec.Pool[random.Next(spec.Pool.Length)])];
            fields.Add(new Field(spec.Name, spec.Type, true));
            arrays.Add(Build(spec.Type, [.. values.Select(v => v is null ? null : spec.Store(v))]));
            model[spec.Name] = values;
        }
        // The struct: a null struct has null fields (as the reader gives them).
        bool[] present = [.. Enumerable.Range(0, length).Select(_ => random.Next(6) != 0)];
        object?[] a = [.. Enumerable.Range(0, length).Select(i => present[i] && random.Next(4) != 0 ? (object)random.Next(-3, 4) : null)];
        object?[] b = [.. Enumerable.Range(0, length).Select(i => present[i] && random.Next(4) != 0 ? Strings[random.Next(Strings.Length)] : null)];
        var validity = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++) if (present[i]) validity[i >> 3] |= (byte)(1 << (i & 7));
        IArrowArray structArray = new StructArray(Struct, length, [Build(Int32Type.Default, [.. a.Select(v => v is null ? null : (object)(long)(int)v)]), Build(StringType.Default, b)],
            new ArrowBuffer(validity), present.Count(p => !p));
        fields.Add(new Field("s", Struct, true));
        arrays.Add(structArray);
        model["s"] = [.. Enumerable.Range(0, length).Select(i => present[i] ? (object)new Dictionary<string, object?> { ["a"] = a[i], ["b"] = b[i] } : null)];
        var schema = new Schema(fields, null);
        return new Table(schema, new RecordBatch(schema, arrays, length), model);
    }

    /// <summary>An array of stored values (long, ulong, double, float, Half, Int128, bool, string; null for null).</summary>
    private static IArrowArray Build(IArrowType type, object?[] stored)
    {
        int n = stored.Length, nulls = stored.Count(v => v is null);
        var validity = new byte[(n + 7) / 8 + 1];
        for (int i = 0; i < n; i++) if (stored[i] is not null) validity[i >> 3] |= (byte)(1 << (i & 7));
        ArrowBuffer valid = new(validity);
        switch (type)
        {
            case StringType:
            {
                var builder = new StringArray.Builder();
                foreach (object? v in stored) { if (v is string s) builder.Append(s); else builder.AppendNull(); }
                return builder.Build();
            }
            case StringViewType:
            {
                var builder = new StringViewArray.Builder();
                foreach (object? v in stored) { if (v is string s) builder.Append(s); else builder.AppendNull(); }
                return builder.Build();
            }
            case DictionaryType d:
            {
                string[] entries = [.. stored.OfType<string>().Distinct()];
                IArrowArray dictionary = Build(d.ValueType, entries);
                int[] indices = [.. stored.Select(v => v is string s ? System.Array.IndexOf(entries, s) : 0)];
                var data = new ArrayData(d, n, nulls, 0, [valid, new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(indices.AsSpan()).ToArray())], [], dictionary.Data);
                return ArrowArrayFactory.BuildArray(data);
            }
            case BooleanType:
            {
                var bits = new byte[(n + 7) / 8 + 1];
                for (int i = 0; i < n; i++) if (stored[i] is true) bits[i >> 3] |= (byte)(1 << (i & 7));
                return ArrowArrayFactory.BuildArray(new ArrayData(type, n, nulls, 0, [valid, new ArrowBuffer(bits)]));
            }
            default:
            {
                int width = ((FixedWidthType)type).BitWidth / 8;
                var bytes = new byte[n * width];
                for (int i = 0; i < n; i++)
                {
                    Span<byte> slot = bytes.AsSpan(i * width, width);
                    switch (stored[i])
                    {
                        case null: break;
                        case double d: BinaryPrimitives.WriteDoubleLittleEndian(slot, d); break;
                        case float f: BinaryPrimitives.WriteSingleLittleEndian(slot, f); break;
                        case Half h: BinaryPrimitives.WriteHalfLittleEndian(slot, h); break;
                        case Int128 x: BinaryPrimitives.WriteInt128LittleEndian(slot, x); break;
                        case ulong u: BinaryPrimitives.WriteUInt64LittleEndian(slot, u); break;
                        case long l:
                            Span<byte> all = stackalloc byte[8];
                            BinaryPrimitives.WriteInt64LittleEndian(all, l);
                            all[..width].CopyTo(slot);
                            break;
                        default: throw new NotSupportedException(stored[i]!.GetType().Name);
                    }
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(type, n, nulls, 0, [valid, new ArrowBuffer(bytes)]));
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Random filters

    /// <summary>A random operand of a spec: the column, a widening cast of it, or (for "s") a struct field.</summary>
    private static (ScalarExpr Operand, Spec Spec) RandomOperand(Random random)
    {
        if (random.Next(10) == 0)
        {
            bool a = random.Next(2) == 0;
            return (Field("s", a ? "a" : "b"), a ? Specs.Single(s => s.Name == "i32") : Specs.Single(s => s.Name == "utf8"));
        }
        Spec spec = Specs[random.Next(Specs.Length)];
        ScalarExpr column = Col(spec.Name);
        if (random.Next(4) == 0)
        {
            IArrowType? wider = spec.Type switch
            {
                Int8Type or Int32Type => Int64Type.Default,
                FloatType => DoubleType.Default,
                UInt16Type => UInt32Type.Default,
                _ => null,
            };
            if (wider is not null) column = ScalarExpr.NewCast(column, Kaminari.Samidare.IR.LogicalType.NewArrow(wider), false);
        }
        return (column, spec);
    }

    /// <summary>A random predicate; <paramref name="supported"/> turns false when it holds a piece lowering drops.</summary>
    private static ScalarExpr RandomPredicate(Random random, int depth, ref bool supported)
    {
        if (depth > 0 && random.Next(3) == 0)
        {
            switch (random.Next(5))
            {
                case 0: return Bin(BinaryOp.And, RandomPredicate(random, depth - 1, ref supported), RandomPredicate(random, depth - 1, ref supported));
                case 1: return Bin(BinaryOp.Or, RandomPredicate(random, depth - 1, ref supported), RandomPredicate(random, depth - 1, ref supported));
                case 2: return Not(RandomPredicate(random, depth - 1, ref supported));
                case 3:
                    return ScalarExpr.NewCoalesce(ListModule.OfSeq([RandomPredicate(random, depth - 1, ref supported),
                        ScalarExpr.NewLiteral(random.Next(3) switch { 0 => LiteralValue.NewBool(true), 1 => LiteralValue.NewBool(false), _ => Lit(null) })]));
                default: return ScalarExpr.NewAlias(RandomPredicate(random, depth - 1, ref supported), "p");
            }
        }
        var (operand, spec) = RandomOperand(random);
        object Literal() => spec.Literals[random.Next(spec.Literals.Length)];
        switch (random.Next(12))
        {
            case 0:
                return ScalarExpr.NewUnary(random.Next(2) == 0 ? UnaryOp.IsNull : UnaryOp.IsNotNull, operand);
            case 1 when spec.Type is HalfFloatType or FloatType or DoubleType:
                return ScalarExpr.NewUnary(UnaryOp.IsNan, operand);
            case 2 when spec.Type is BooleanType:
                return operand;
            case 3 when spec.Type is StringType or StringViewType or DictionaryType:
            {
                object? prefix = random.Next(8) == 0 ? null : ((string)Literal())[..];
                if (prefix is string p) prefix = p[..random.Next(p.Length + 1)];
                return Bin(BinaryOp.StartsWith, operand, ScalarExpr.NewLiteral(Lit(prefix)));
            }
            case 4 or 5:
            {
                var items = Enumerable.Range(0, random.Next(0, 6)).Select(_ => random.Next(6) == 0 ? Lit(null) : Lit(Literal()));
                return ScalarExpr.NewInList(operand, InListSource.NewLiterals(ListModule.OfSeq(items)), random.Next(2) == 0, random.Next(2) == 0);
            }
            case 6 when spec.Foreign.Length > 0:
            {
                // A literal of another kind: the conjunct is dropped.
                supported = false;
                return Bin(Comparisons[random.Next(6)], operand, ScalarExpr.NewLiteral(Lit(spec.Foreign[random.Next(spec.Foreign.Length)])));
            }
            case 7:
            {
                // A column against a column: not lowered.
                supported = false;
                return Bin(Comparisons[random.Next(6)], operand, operand);
            }
            default:
            {
                ScalarExpr literal = ScalarExpr.NewLiteral(random.Next(10) == 0 ? Lit(null) : Lit(Literal()));
                BinaryOp op = Comparisons[random.Next(Comparisons.Length)];
                return random.Next(3) == 0 ? Bin(op, literal, operand) : Bin(op, operand, literal);
            }
        }
    }

    private static int[] Selected(Table table, LoweredFilter lowered)
    {
        if (!OptionModule.IsSome(lowered.Program)) return [.. Enumerable.Range(0, table.Length)];
        FilterProgram program = lowered.Program.Value;
        IArrowArray[] arrays = [.. program.Columns.Select(c => Resolve(table.Batch, c.Path))];
        var mask = new ulong[Masks.Words(table.Length)];
        program.Evaluate(arrays, table.Length, mask);
        return [.. Enumerable.Range(0, table.Length).Where(i => (mask[i >> 6] >> (i & 63) & 1) != 0)];
    }

    private static IArrowArray Resolve(RecordBatch batch, string[] path)
    {
        IArrowArray array = batch.Column(path[0]);
        foreach (string name in path[1..])
        {
            var parent = (StructArray)array;
            array = parent.Fields[((StructType)parent.Data.DataType).GetFieldIndex(name)];
        }
        return array;
    }

    private static int[] Truth(Table table, ScalarExpr filter) =>
        [.. Enumerable.Range(0, table.Length).Where(i => IrEvaluator.Eval(filter, name => table.Model.TryGetValue(name, out object?[]? v) ? v[i] : null) is true)];

    private static string Show(ScalarExpr e) => e.ToString().Replace("\n", " ");

    [Fact]
    public void RandomFilters_SelectWhatTheyKeep()
    {
        var random = new Random(15112026);
        Table table = RandomTable(random, 400);
        int exactRounds = 0, droppedRounds = 0, partial = 0;
        for (int round = 0; round < 3000; round++)
        {
            bool supported = true;
            int conjuncts = random.Next(1, 4);
            List<bool> each = [];
            ScalarExpr filter = null!;
            for (int c = 0; c < conjuncts; c++)
            {
                bool ok = true;
                ScalarExpr conjunct = RandomPredicate(random, 2, ref ok);
                each.Add(ok);
                filter = c == 0 ? conjunct : Bin(BinaryOp.And, filter, conjunct);
            }
            supported = each.All(x => x);
            LoweredFilter lowered = FilterLowering.lower(table.Schema, filter);
            int[] truth = Truth(table, filter), got = Selected(table, lowered);
            string where = $"round {round}: {Show(filter)}";
            if (truth.Length > 0 && truth.Length < table.Length) partial++;
            Assert.True(supported == lowered.Exact, $"{where}: exact {lowered.Exact}, expected {supported}");
            if (lowered.Exact)
            {
                Assert.True(truth.SequenceEqual(got), $"{where}\nexpected {string.Join(",", truth)}\ngot      {string.Join(",", got)}");
                exactRounds++;
            }
            else
            {
                Assert.True(truth.Except(got).Count() == 0, $"{where}: lost rows {string.Join(",", truth.Except(got))}");
                droppedRounds++;
            }
        }
        Assert.True(exactRounds > 1000 && droppedRounds > 300 && partial > 1500,
            $"{exactRounds} exact, {droppedRounds} with dropped conjuncts, {partial} keeping some rows but not all");
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Rewrites, by hand

    private static (Table Table, LoweredFilter Lowered) Lower(ScalarExpr filter)
    {
        Table table = RandomTable(new Random(3), 200);
        return (table, FilterLowering.lower(table.Schema, filter));
    }

    private static void AssertExact(ScalarExpr filter)
    {
        var (table, lowered) = Lower(filter);
        Assert.True(lowered.Exact, Show(filter));
        Assert.Equal(Truth(table, filter), Selected(table, lowered));
    }

    [Fact]
    public void LiteralsBetweenOrOutsideStoredValues_KeepTheirOutcome()
    {
        ScalarExpr i8 = Col("i8"), f32 = Col("f32"), ts = Col("ts_ms");
        foreach (BinaryOp op in Comparisons)
        {
            AssertExact(Bin(op, i8, ScalarExpr.NewLiteral(Lit(1000))));      // above every Int8
            AssertExact(Bin(op, i8, ScalarExpr.NewLiteral(Lit(-1000))));     // below
            AssertExact(Bin(op, i8, ScalarExpr.NewLiteral(Lit(2.5))));       // between 2 and 3
            AssertExact(Not(Bin(op, i8, ScalarExpr.NewLiteral(Lit(2.5)))));  // null stays null under NOT
            AssertExact(Bin(op, f32, ScalarExpr.NewLiteral(Lit(0.1))));      // no float is 0.1
            AssertExact(Bin(op, f32, ScalarExpr.NewLiteral(Lit(1e300))));    // beyond every finite float
            AssertExact(Bin(op, ts, ScalarExpr.NewLiteral(Lit(T0.AddTicks(1)))));
        }
    }

    [Fact]
    public void DroppedConjuncts_TakeTheirColumnsAlong()
    {
        // i8 > 1 AND (f64 < f64): the second conjunct is no comparison with a literal.
        ScalarExpr filter = Bin(BinaryOp.And, Bin(BinaryOp.Gt, Col("i8"), ScalarExpr.NewLiteral(Lit(1))), Bin(BinaryOp.Lt, Col("f64"), Col("f64")));
        var (_, lowered) = Lower(filter);
        Assert.False(lowered.Exact);
        Assert.Equal(["i8"], lowered.Program.Value.Columns.Select(c => string.Join('.', c.Path)));

        // A conjunct failing after it named a column takes the column back.
        ScalarExpr late = Bin(BinaryOp.And, Bin(BinaryOp.Gt, Col("i8"), ScalarExpr.NewLiteral(Lit(1))),
            Bin(BinaryOp.Or, Bin(BinaryOp.Eq, Col("f64"), ScalarExpr.NewLiteral(Lit(1.0))), Bin(BinaryOp.Lt, Col("f32"), Col("f32"))));
        var (_, trimmed) = Lower(late);
        Assert.False(trimmed.Exact);
        Assert.Equal(["i8"], trimmed.Program.Value.Columns.Select(c => string.Join('.', c.Path)));

        var (_, none) = Lower(Bin(BinaryOp.Lt, Col("f64"), Col("f64")));
        Assert.False(none.Exact);
        Assert.True(OptionModule.IsNone(none.Program));

        // A column read by several conjuncts is read once; struct fields by their path.
        ScalarExpr twice = Bin(BinaryOp.And, Bin(BinaryOp.Gt, Col("i8"), ScalarExpr.NewLiteral(Lit(1))),
            Bin(BinaryOp.And, Bin(BinaryOp.Lt, Col("i8"), ScalarExpr.NewLiteral(Lit(5))), Bin(BinaryOp.Eq, Field("s", "a"), ScalarExpr.NewLiteral(Lit(2)))));
        var (table, both) = Lower(twice);
        Assert.True(both.Exact);
        Assert.Equal(["i8", "s.a"], both.Program.Value.Columns.Select(c => string.Join('.', c.Path)));
        Assert.Equal(Truth(table, twice), Selected(table, both));
    }

    [Fact]
    public void UnknownColumnsAndCastsThatChangeValues_AreDropped()
    {
        AssertDropped(Bin(BinaryOp.Eq, Col("nope"), ScalarExpr.NewLiteral(Lit(1))));
        AssertDropped(Bin(BinaryOp.Eq, Col("s"), ScalarExpr.NewLiteral(Lit(1))));                 // a struct, not a leaf
        AssertDropped(Bin(BinaryOp.Eq, Field("s", "zz"), ScalarExpr.NewLiteral(Lit(1))));
        AssertDropped(Bin(BinaryOp.Lt, ScalarExpr.NewCast(Col("i64"), Kaminari.Samidare.IR.LogicalType.NewArrow(Int32Type.Default), false),
            ScalarExpr.NewLiteral(Lit(1))));                                                       // narrowing
        AssertDropped(ScalarExpr.NewUnary(UnaryOp.IsNan, Col("i32")));
        AssertDropped(Bin(BinaryOp.StartsWith, Col("i32"), ScalarExpr.NewLiteral(Lit("1"))));
        AssertDropped(Bin(BinaryOp.Eq, Col("utf8"), ScalarExpr.NewLiteral(Lit(1))));

        static void AssertDropped(ScalarExpr filter)
        {
            var (_, lowered) = Lower(filter);
            Assert.False(lowered.Exact, Show(filter));
            Assert.True(OptionModule.IsNone(lowered.Program), Show(filter));
        }
    }
}
