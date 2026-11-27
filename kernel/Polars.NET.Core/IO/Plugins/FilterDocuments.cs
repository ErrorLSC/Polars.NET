using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;

namespace Polars.NET.Core.IO.Plugins;

/// <summary>
/// The engine's pushed filters (<c>filter_expr.v1</c> JSON documents of lazyframe_resolver.v1) as Samidare IR.
/// A node with no faithful IR form (an <c>unknown</c>, a literal the IR cannot hold exactly, a dtype it does not
/// name) has no translation, and neither has any node above it; the <c>and</c>s around it still give an implied
/// predicate (<see cref="PushedFilter.Implied"/>). Casts are read as non-strict: the document does not say which.
/// </summary>
internal static class FilterDocuments
{
    /// <summary>The document's exact translation and a predicate it implies (each null when there is none).</summary>
    public static (ScalarExpr? Exact, ScalarExpr? Implied) Translate(string document)
    {
        try
        {
            using JsonDocument json = JsonDocument.Parse(document);
            ScalarExpr? exact = Node(json.RootElement);
            return (exact, exact ?? Implied(json.RootElement));
        }
        catch (Exception e) when (e is JsonException or FormatException or OverflowException or InvalidOperationException
                                       or KeyNotFoundException or ArgumentException)
        {
            // A document this translator does not read is a filter it cannot use: the engine applies it.
            return (null, null);
        }
    }

    /// <summary>The conjuncts of the <c>and</c>s that translate (true wherever the node is true), or null.</summary>
    private static ScalarExpr? Implied(JsonElement node)
    {
        if (Node(node) is { } exact) return exact;
        if (Op(node) != "and") return null;
        JsonElement args = node.GetProperty("args");
        ScalarExpr? left = Implied(args[0]), right = Implied(args[1]);
        return (left, right) switch
        {
            ({ } l, { } r) => ScalarExpr.NewBinary(BinaryOp.And, l, r),
            ({ } l, null) => l,
            (null, { } r) => r,
            _ => null,
        };
    }

    private static string? Op(JsonElement node) =>
        node.ValueKind == JsonValueKind.Object && node.TryGetProperty("op", out JsonElement op) ? op.GetString() : null;

    private static ScalarExpr? Node(JsonElement node)
    {
        if (node.ValueKind != JsonValueKind.Object) return null;
        if (node.TryGetProperty("col", out JsonElement column)) return ScalarExpr.NewColumn(column.GetString()!);
        if (node.TryGetProperty("lit", out JsonElement literal)) return Literal(literal) is { } value ? ScalarExpr.NewLiteral(value) : null;
        if (!node.TryGetProperty("op", out JsonElement opElement)) return null;
        string op = opElement.GetString()!;
        JsonElement args = node.GetProperty("args");
        var operands = new ScalarExpr[args.GetArrayLength()];
        for (int i = 0; i < operands.Length; i++)
        {
            if (Node(args[i]) is not { } operand) return null;
            operands[i] = operand;
        }
        return (op, operands.Length) switch
        {
            ("and", 2) => Binary(BinaryOp.And),
            ("or", 2) => Binary(BinaryOp.Or),
            ("eq", 2) => Binary(BinaryOp.Eq),
            ("neq", 2) => Binary(BinaryOp.NotEq),
            ("lt", 2) => Binary(BinaryOp.Lt),
            ("lt_eq", 2) => Binary(BinaryOp.LtEq),
            ("gt", 2) => Binary(BinaryOp.Gt),
            ("gt_eq", 2) => Binary(BinaryOp.GtEq),
            ("eq_missing", 2) => Binary(BinaryOp.EqMissing),
            ("neq_missing", 2) => Binary(BinaryOp.NotEqMissing),
            ("not", 1) => ScalarExpr.NewUnary(UnaryOp.Not, operands[0]),
            ("is_null", 1) => ScalarExpr.NewUnary(UnaryOp.IsNull, operands[0]),
            ("is_not_null", 1) => ScalarExpr.NewUnary(UnaryOp.IsNotNull, operands[0]),
            ("is_between", 3) => Between(node.GetProperty("closed").GetString()),
            ("is_in", 1) => In(node),
            ("starts_with", 2) => Binary(BinaryOp.StartsWith),
            ("ends_with", 2) => Binary(BinaryOp.EndsWith),
            ("contains_literal", 2) => ScalarExpr.NewFunction(ScalarFunction.StrContains,
                ListModule.OfArray([operands[0], operands[1], ScalarExpr.NewLiteral(LiteralValue.NewBool(true))]), LogicalTypes.Boolean),
            ("cast", 1) => DataType(node.GetProperty("dtype").GetString()!) is { } target ? ScalarExpr.NewCast(operands[0], target, false) : null,
            _ => null,
        };

        ScalarExpr Binary(BinaryOp binary) => ScalarExpr.NewBinary(binary, operands[0], operands[1]);

        ScalarExpr? Between(string? closed)
        {
            (BinaryOp lower, BinaryOp upper)? ops = closed switch
            {
                "both" => (BinaryOp.GtEq, BinaryOp.LtEq),
                "left" => (BinaryOp.GtEq, BinaryOp.Lt),
                "right" => (BinaryOp.Gt, BinaryOp.LtEq),
                "none" => (BinaryOp.Gt, BinaryOp.Lt),
                _ => null,
            };
            if (ops is not (var lower, var upper)) return null;
            return ScalarExpr.NewBinary(BinaryOp.And,
                ScalarExpr.NewBinary(lower, operands[0], operands[1]),
                ScalarExpr.NewBinary(upper, operands[0], operands[2]));
        }

        ScalarExpr? In(JsonElement inNode)
        {
            JsonElement values = inNode.GetProperty("values");
            string dtype = values.GetProperty("dtype").GetString()!;
            JsonElement items = values.GetProperty("values");
            var literals = new LiteralValue[items.GetArrayLength()];
            for (int i = 0; i < literals.Length; i++)
            {
                if (Value(dtype, items[i]) is not { } value) return null;
                literals[i] = value;
            }
            bool nullsEqual = inNode.GetProperty("nulls_equal").GetBoolean();
            return ScalarExpr.NewInList(operands[0], InListSource.NewLiterals(ListModule.OfArray(literals)), false, nullsEqual);
        }
    }

    /// <summary>A literal node's value, or null when the IR cannot hold it exactly.</summary>
    private static LiteralValue? Literal(JsonElement literal) =>
        literal.ValueKind == JsonValueKind.Object
        && literal.TryGetProperty("dtype", out JsonElement dtype)
        && literal.TryGetProperty("value", out JsonElement value)
            ? Value(dtype.GetString()!, value)
            : null;

    /// <summary>A value of the given dtype; a null value is a typed null. Null when the IR cannot hold it exactly.</summary>
    private static LiteralValue? Value(string dtype, JsonElement value)
    {
        if (dtype == "null") return LiteralValue.NewNull(LogicalType.Unknown);
        // "Any other value of v is null" (the contract), and so is a JSON null.
        bool isNull = dtype switch
        {
            "bool" => value.ValueKind is not (JsonValueKind.True or JsonValueKind.False),
            "date" => value.ValueKind != JsonValueKind.Number,
            _ => value.ValueKind != JsonValueKind.String,
        };
        if (isNull) return DataType(dtype) is { } type ? LiteralValue.NewNull(type) : null;
        string text = dtype is "bool" or "date" ? "" : value.GetString()!;
        return dtype switch
        {
            "bool" => LiteralValue.NewBool(value.GetBoolean()),
            "str" => LiteralValue.NewString(text),
            "i8" => LiteralValue.NewInt8(sbyte.Parse(text, CultureInfo.InvariantCulture)),
            "i16" => LiteralValue.NewInt16(short.Parse(text, CultureInfo.InvariantCulture)),
            "i32" => LiteralValue.NewInt32(int.Parse(text, CultureInfo.InvariantCulture)),
            "i64" => LiteralValue.NewInt64(long.Parse(text, CultureInfo.InvariantCulture)),
            "u8" => LiteralValue.NewUInt8(byte.Parse(text, CultureInfo.InvariantCulture)),
            "u16" => LiteralValue.NewUInt16(ushort.Parse(text, CultureInfo.InvariantCulture)),
            "u32" => LiteralValue.NewUInt32(uint.Parse(text, CultureInfo.InvariantCulture)),
            "u64" => LiteralValue.NewUInt64(ulong.Parse(text, CultureInfo.InvariantCulture)),
            "f32" => LiteralValue.NewFloat32((float)Float(text)),
            "f64" => LiteralValue.NewFloat64(Float(text)),
            "date" => LiteralValue.NewDate(DateOnly.FromDayNumber(checked(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + value.GetInt32()))),
            "time" => Ticks(text, 1) is { } ticks ? LiteralValue.NewTimeOnly(new TimeOnly(ticks)) : null,
            _ when Temporal(dtype) is (var kind, var unit, var zone) => (kind, Ticks(text, unit)) switch
            {
                (_, null) => null,
                ("duration", { } t) => LiteralValue.NewTimeSpan(new TimeSpan(t)),
                // A naive timestamp: the wall clock it holds.
                ("datetime", { } t) when zone is null => LiteralValue.NewDateTime(new DateTime(DateTime.UnixEpoch.Ticks + t, DateTimeKind.Unspecified)),
                // An instant in UTC; other zones keep their zone in the column type, which the IR literal does not carry.
                ("datetime", { } t) when zone == "UTC" => LiteralValue.NewDateTimeOffset(new DateTimeOffset(DateTime.UnixEpoch.Ticks + t, TimeSpan.Zero)),
                _ => null,
            },
            _ when Decimal(dtype) is (_, var scale) => DecimalValue(text, scale),
            _ => null,
        };

        static double Float(string text) => text switch
        {
            "NaN" => double.NaN,
            "inf" => double.PositiveInfinity,
            "-inf" => double.NegativeInfinity,
            _ => double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture),
        };

        // A count of `nanosPerUnit`-nanosecond units as 100 ns ticks; null when it is not a whole number of ticks.
        static long? Ticks(string text, long nanosPerUnit)
        {
            BigInteger nanos = BigInteger.Parse(text, CultureInfo.InvariantCulture) * nanosPerUnit;
            BigInteger ticks = BigInteger.DivRem(nanos, 100, out BigInteger rest);
            return rest.IsZero && ticks >= long.MinValue && ticks <= long.MaxValue ? (long)ticks : null;
        }

        static LiteralValue? DecimalValue(string unscaledText, int scale)
        {
            BigInteger unscaled = BigInteger.Parse(unscaledText, CultureInfo.InvariantCulture);
            // System.Decimal holds 96-bit unscaled values with scales up to 28.
            if (scale is < 0 or > 28 || BigInteger.Abs(unscaled) >= BigInteger.One << 96) return null;
            var magnitude = BigInteger.Abs(unscaled);
            return LiteralValue.NewDecimal(new decimal((int)(uint)(magnitude & uint.MaxValue), (int)(uint)((magnitude >> 32) & uint.MaxValue),
                (int)(uint)((magnitude >> 64) & uint.MaxValue), unscaled.Sign < 0, (byte)scale));
        }
    }

    /// <summary>A document dtype as an IR type; null for the dtypes the IR does not name here.</summary>
    internal static LogicalType? DataType(string dtype) => dtype switch
    {
        "bool" => LogicalTypes.Boolean,
        "str" => LogicalTypes.String,
        "i8" => LogicalTypes.Int8,
        "i16" => LogicalType.NewArrow(Int16Type.Default),
        "i32" => LogicalType.NewArrow(Int32Type.Default),
        "i64" => LogicalTypes.Int64,
        "u8" => LogicalType.NewArrow(UInt8Type.Default),
        "u16" => LogicalType.NewArrow(UInt16Type.Default),
        "u32" => LogicalType.NewArrow(UInt32Type.Default),
        "u64" => LogicalTypes.UInt64,
        "f32" => LogicalTypes.Float32,
        "f64" => LogicalTypes.Float64,
        "date" => LogicalTypes.Date,
        "time" => LogicalTypes.Time,
        _ when Temporal(dtype) is (var kind, var unit, var zone) => kind switch
        {
            "datetime" => LogicalType.NewArrow(new TimestampType(Unit(unit), zone)),
            _ => LogicalType.NewArrow(unit switch { 1 => DurationType.Nanosecond, 1_000 => DurationType.Microsecond, _ => DurationType.Millisecond }),
        },
        _ when Decimal(dtype) is (var precision, var scale) => LogicalType.NewArrow(new Decimal128Type(precision, scale)),
        _ => null,
    };

    private static TimeUnit Unit(long nanosPerUnit) => nanosPerUnit switch
    {
        1 => TimeUnit.Nanosecond,
        1_000 => TimeUnit.Microsecond,
        _ => TimeUnit.Millisecond,
    };

    /// <summary><c>datetime[unit]</c>, <c>datetime[unit, zone]</c> or <c>duration[unit]</c>: the kind, the unit in
    /// nanoseconds and the zone.</summary>
    private static (string Kind, long NanosPerUnit, string? Zone)? Temporal(string dtype)
    {
        string kind;
        if (dtype.StartsWith("datetime[", StringComparison.Ordinal)) kind = "datetime";
        else if (dtype.StartsWith("duration[", StringComparison.Ordinal)) kind = "duration";
        else return null;
        if (!dtype.EndsWith(']')) return null;
        string inner = dtype[(kind.Length + 1)..^1];
        int comma = inner.IndexOf(',');
        string unitText = (comma < 0 ? inner : inner[..comma]).Trim();
        string? zone = comma < 0 ? null : inner[(comma + 1)..].Trim();
        if (kind == "duration" && zone is not null) return null;
        long? unit = unitText switch
        {
            "ns" => 1,
            "us" => 1_000,
            "ms" => 1_000_000,
            _ => null,
        };
        return unit is { } u ? (kind, u, zone) : null;
    }

    /// <summary><c>decimal[precision, scale]</c>.</summary>
    private static (int Precision, int Scale)? Decimal(string dtype)
    {
        if (!dtype.StartsWith("decimal[", StringComparison.Ordinal) || !dtype.EndsWith(']')) return null;
        string[] parts = dtype["decimal[".Length..^1].Split(',', StringSplitOptions.TrimEntries);
        return parts.Length == 2
               && int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out int precision)
               && int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int scale)
            ? (precision, scale)
            : null;
    }
}
