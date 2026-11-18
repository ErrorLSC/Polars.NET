using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.FSharp.Core;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Marshalling;

namespace Kaminari.Sazanami.Data;

/// <summary>
/// Builds Arrow record batches from rows of <typeparamref name="T"/> straight into native memory: the mirror of
/// <see cref="ArrowRowMapper{T}"/>. A record, class or struct (or F# record) gives one column per public readable
/// property, in declaration order; any other <typeparamref name="T"/> (a number, a string, a list, ...) one column
/// named <c>value</c>. The values are appended by code compiled once per type, without boxing.
/// <para>
/// Layout (the engine's): integers, floats and <see cref="Half"/> as themselves; <see cref="decimal"/> as
/// decimal(38, 18) (a value that does not fit exactly is refused); <see cref="DateTime"/> as a timestamp in
/// microseconds without time zone (its wall clock; <see cref="DateTime.Kind"/> ignored), <see cref="DateTimeOffset"/>
/// as a UTC timestamp in microseconds, <see cref="TimeSpan"/> as a duration in microseconds, <see cref="TimeOnly"/> as
/// time64 in nanoseconds, <see cref="DateOnly"/> as date32; strings and chars as string views, <c>byte[]</c> and
/// <see cref="Guid"/> as binary views; enums as a dictionary of their member names carrying the engine's Enum metadata
/// ([Flags] enums as their underlying integers); collections (dictionaries included, as lists of Key/Value structs)
/// as large lists; nested records and classes as structs. A type with nothing of that (object, a class without
/// readable properties) is written as its <see cref="object.ToString"/>. Value types are non-nullable fields;
/// reference types, <see cref="Nullable{T}"/> and F# options nullable ones.
/// </para>
/// </summary>
/// <remarks>
/// A row that fails to append (a value the layout refuses) leaves the builder unusable: every later call throws.
/// </remarks>
public sealed class ArrowRowBuilder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable
{
    private static readonly Lazy<RowPlan> Plan = new(RowPlan.Create);

    private readonly NativeStructBuilder _root;
    private readonly ArrowValueWriter[] _writers;
    private readonly Action<T, ArrowValueWriter[]> _append;
    private readonly bool _rowsAreRecords;
    private bool _faulted;

    public ArrowRowBuilder()
    {
        RowPlan plan = Plan.Value;
        _root = (NativeStructBuilder)NativeColumnBuilder.Create(new StructType(plan.Schema.FieldsList));
        _writers = [.. plan.Columns.Select((c, i) => c.CreateWriter(_root.Children[i]))];
        _append = plan.Append;
        _rowsAreRecords = plan.RowsAreRecords;
    }

    /// <summary>The schema of the batches: the columns of <typeparamref name="T"/>.</summary>
    public static Schema Schema => Plan.Value.Schema;

    /// <summary>Whether <typeparamref name="T"/> is laid out as records (one column per property; a null row is refused).</summary>
    public static bool RowsAreRecords => Plan.Value.RowsAreRecords;

    /// <summary>The rows appended since the last <see cref="ExportBatch"/>.</summary>
    public int Count => (int)_root.Length;

    /// <summary>Expects <paramref name="rows"/> more rows in this batch (the top-level buffers grow once).</summary>
    public void Reserve(int rows)
    {
        foreach (var child in _root.Children) child.Reserve(rows);
    }

    /// <summary>Appends a row.</summary>
    /// <exception cref="ArgumentException"><paramref name="row"/> is null (when rows are records).</exception>
    /// <exception cref="OverflowException">A decimal does not fit decimal(38, 18) exactly.</exception>
    public void Append(T row)
    {
        ThrowIfFaulted();
        if (_rowsAreRecords && row is null) throw new ArgumentException($"A null {typeof(T).Name} row cannot be appended (row {Count} of the batch).", nameof(row));
        try
        {
            _append(row, _writers);
        }
        catch
        {
            _faulted = true;
            throw;
        }
        _root.EndStruct();
    }

    /// <summary>Appends rows.</summary>
    /// <inheritdoc cref="Append(T)"/>
    public void AppendRange(ReadOnlySpan<T> rows)
    {
        foreach (T row in rows) Append(row);
    }

    /// <summary>
    /// The rows appended since the last export as a batch over native memory (freed when the batch is disposed);
    /// the builder starts the next batch empty.
    /// </summary>
    public RecordBatch ExportBatch()
    {
        ThrowIfFaulted();
        return _root.ExportBatch(Plan.Value.Schema);
    }

    public void Dispose() => _root.Dispose();

    private void ThrowIfFaulted()
    {
        if (_faulted) throw new InvalidOperationException("A row failed to append; the builder holds a partial row and cannot be used further.");
    }

    private sealed class RowPlan(Schema schema, RowNode[] columns, Action<T, ArrowValueWriter[]> append, bool rowsAreRecords)
    {
        public Schema Schema { get; } = schema;
        public RowNode[] Columns { get; } = columns;
        public Action<T, ArrowValueWriter[]> Append { get; } = append;
        public bool RowsAreRecords { get; } = rowsAreRecords;

        public static RowPlan Create()
        {
            if (RowLayout.Members(typeof(T)) is { } members)
            {
                var (nodes, append) = RowLayout.CompileMembers<T>(members);
                Field[] fields = [.. members.Select((m, i) => nodes[i].Field(m.Name))];
                return new RowPlan(new Schema(fields, null), nodes, append, rowsAreRecords: true);
            }
            RowNode node = RowLayout.Resolve(typeof(T));
            return new RowPlan(new Schema([node.Field("value")], null), [node],
                static (row, writers) => ((ArrowValueWriter<T>)writers[0]).Write(row), rowsAreRecords: false);
        }
    }
}

/// <summary>How one .NET type is laid out: its Arrow type, and how to make a writer over a builder of that type.</summary>
internal sealed class RowNode(IArrowType type, bool nullable, Dictionary<string, string>? metadata, Type writerType,
    Func<NativeColumnBuilder, ArrowValueWriter> createWriter)
{
    public IArrowType Type { get; } = type;
    public bool Nullable { get; } = nullable;
    public Dictionary<string, string>? Metadata { get; } = metadata;

    /// <summary>The concrete (sealed) writer type, for direct calls from compiled code.</summary>
    public Type WriterType { get; } = writerType;

    public Func<NativeColumnBuilder, ArrowValueWriter> CreateWriter { get; } = createWriter;

    public Field Field(string name) => new(name, Type, Nullable, Metadata);
}

/// <summary>The .NET → Arrow layout of <see cref="ArrowRowBuilder{T}"/>.</summary>
internal static class RowLayout
{
    private static readonly ConcurrentDictionary<Type, RowNode> Nodes = new();
    private static readonly DurationType Micros = DurationType.Microsecond;

    [ThreadStatic] private static HashSet<Type>? t_resolving;

    /// <summary>The properties a record type is laid out as, or null when the type is not a record (one value column).</summary>
    public static PropertyInfo[]? Members([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type type)
    {
        if (ClrTypeHelper.GetOptionalInfo(type).Kind != OptionalKind.None || Scalar(type) is not null || type == typeof(object)
            || type.IsInterface || type.IsAbstract || ElementType(type) is not null)
            return null;
        PropertyInfo[] members = ClrTypeHelper.GetModelProperties(type);
        return members.Length > 0 ? members : null;
    }

    public static RowNode Resolve([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type type)
    {
        if (Nodes.TryGetValue(type, out RowNode? node)) return node;
        HashSet<Type> resolving = t_resolving ??= [];
        if (!resolving.Add(type))
            throw new NotSupportedException($"Type {type.FullName} contains itself; recursive types have no Arrow layout.");
        try
        {
            return Nodes.GetOrAdd(type, Build(type));
        }
        finally
        {
            resolving.Remove(type);
        }
    }

    private static RowNode Build([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type type)
    {
        var (core, kind) = ClrTypeHelper.GetOptionalInfo(type);
        if (kind != OptionalKind.None)
        {
            RowNode inner = Resolve(core);
            Dictionary<string, string>? metadata = Metadata(core);
            if (kind == OptionalKind.Nullable && Scalar(core) is { Converter: { } conv } fixedScalar)
            {
                Type nullableFixed = typeof(NullableFixedWriter<,,>).MakeGenericType(core, fixedScalar.Stored!, conv);
                return new RowNode(inner.Type, true, metadata, nullableFixed, b => (ArrowValueWriter)Activator.CreateInstance(nullableFixed, b)!);
            }
            Type wrapper = (kind switch
            {
                OptionalKind.Nullable => typeof(NullableWriter<>),
                OptionalKind.FSharpOption => typeof(OptionWriter<>),
                _ => typeof(ValueOptionWriter<>),
            }).MakeGenericType(core);
            return new RowNode(inner.Type, true, metadata, wrapper, b => (ArrowValueWriter)Activator.CreateInstance(wrapper, inner.CreateWriter(b))!);
        }

        bool nullable = !type.IsValueType;
        if (Scalar(type) is { } scalar)
        {
            Type writer = scalar.Converter is { } conv
                ? typeof(FixedWriter<,,>).MakeGenericType(type, scalar.Stored!, conv)
                : scalar.Writer!;
            return new RowNode(scalar.Type, nullable, Metadata(type), writer, b => (ArrowValueWriter)Activator.CreateInstance(writer, b)!);
        }

        if (ElementType(type) is { } element)
        {
            RowNode item = Resolve(element);
            Type writer = typeof(ListWriter<,>).MakeGenericType(type, element);
            return new RowNode(new LargeListType(item.Field("item")), nullable, null, writer,
                b => (ArrowValueWriter)Activator.CreateInstance(writer, b, item.CreateWriter(((NativeLargeListBuilder)b).Values))!);
        }

        if (Members(type) is { } members)
        {
            MethodInfo compile = typeof(RowLayout).GetMethod(nameof(CompileStruct), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(type);
            (RowNode[] nodes, Delegate append) compiled;
            try
            {
                compiled = ((RowNode[], Delegate))compile.Invoke(null, [members])!;
            }
            catch (TargetInvocationException e) when (e.InnerException is { } inner)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(inner);
                throw;
            }
            var (nodes, append) = compiled;
            Type writer = typeof(StructWriter<>).MakeGenericType(type);
            var structType = new StructType([.. members.Select((m, i) => nodes[i].Field(m.Name))]);
            return new RowNode(structType, nullable, null, writer, b =>
            {
                var builder = (NativeStructBuilder)b;
                ArrowValueWriter[] children = [.. nodes.Select((n, i) => n.CreateWriter(builder.Children[i]))];
                return (ArrowValueWriter)Activator.CreateInstance(writer, builder, children, append)!;
            });
        }

        // Nothing to lay out: the value's text.
        Type text = typeof(ToStringWriter<>).MakeGenericType(type);
        return new RowNode(StringViewType.Default, nullable, null, text, b => (ArrowValueWriter)Activator.CreateInstance(text, b)!);
    }

    /// <summary>
    /// The nodes of <paramref name="members"/> and the code appending them: for each one,
    /// <c>((Writer)writers[i]).Write(row.Member)</c>, a direct call on the member's concrete writer.
    /// </summary>
    public static (RowNode[] Nodes, Action<TRow, ArrowValueWriter[]> Append) CompileMembers<TRow>(PropertyInfo[] members)
    {
        RowNode[] nodes = [.. members.Select(m => Resolve(m.PropertyType))];
        ParameterExpression row = Expression.Parameter(typeof(TRow), "row");
        ParameterExpression writers = Expression.Parameter(typeof(ArrowValueWriter[]), "writers");
        Expression[] calls = [.. members.Select((m, i) => (Expression)Expression.Call(
            Expression.Convert(Expression.ArrayIndex(writers, Expression.Constant(i)), nodes[i].WriterType),
            nodes[i].WriterType.GetMethod(nameof(ArrowValueWriter<int>.Write), [m.PropertyType])!,
            Expression.Property(row, m)))];
        Expression body = calls.Length == 0 ? Expression.Empty() : Expression.Block(calls);
        return (nodes, Expression.Lambda<Action<TRow, ArrowValueWriter[]>>(body, row, writers).Compile());
    }

    private static (RowNode[] Nodes, Delegate Append) CompileStruct<TStruct>(PropertyInfo[] members) => CompileMembers<TStruct>(members);

    /// <summary>The element type when <paramref name="type"/> is laid out as a list (strings and byte arrays are not).</summary>
    private static Type? ElementType([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type type) =>
        type == typeof(string) || type == typeof(byte[]) ? null : ClrTypeHelper.TryGetEnumerableElementType(type);

    /// <summary>A scalar's layout: fixed-width values through a converter, or a writer of its own.</summary>
    private sealed record ScalarLayout(IArrowType Type, Type? Stored = null, Type? Converter = null, Type? Writer = null);

    private static ScalarLayout Fixed<TValue>(IArrowType type) where TValue : unmanaged =>
        new(type, typeof(TValue), typeof(Same<TValue>));

    private static readonly FrozenDictionary<Type, ScalarLayout> Scalars = new Dictionary<Type, ScalarLayout>
    {
        [typeof(sbyte)] = Fixed<sbyte>(Int8Type.Default),
        [typeof(short)] = Fixed<short>(Int16Type.Default),
        [typeof(int)] = Fixed<int>(Int32Type.Default),
        [typeof(long)] = Fixed<long>(Int64Type.Default),
        [typeof(byte)] = Fixed<byte>(UInt8Type.Default),
        [typeof(ushort)] = Fixed<ushort>(UInt16Type.Default),
        [typeof(uint)] = Fixed<uint>(UInt32Type.Default),
        [typeof(ulong)] = Fixed<ulong>(UInt64Type.Default),
        [typeof(Half)] = Fixed<Half>(HalfFloatType.Default),
        [typeof(float)] = Fixed<float>(FloatType.Default),
        [typeof(double)] = Fixed<double>(DoubleType.Default),
        [typeof(decimal)] = new(new Decimal128Type(38, 18), typeof(Int128), typeof(DecimalToScaled)),
        [typeof(DateTime)] = new(new TimestampType(TimeUnit.Microsecond, (string?)null), typeof(long), typeof(DateTimeToMicros)),
        [typeof(DateTimeOffset)] = new(new TimestampType(TimeUnit.Microsecond, "UTC"), typeof(long), typeof(DateTimeOffsetToMicros)),
        [typeof(TimeSpan)] = new(Micros, typeof(long), typeof(TimeSpanToMicros)),
        [typeof(TimeOnly)] = new(new Time64Type(TimeUnit.Nanosecond), typeof(long), typeof(TimeOnlyToNanos)),
        [typeof(DateOnly)] = new(Date32Type.Default, typeof(int), typeof(DateOnlyToDays)),
        [typeof(bool)] = new(BooleanType.Default, Writer: typeof(BooleanWriter)),
        [typeof(string)] = new(StringViewType.Default, Writer: typeof(StringWriter)),
        [typeof(char)] = new(StringViewType.Default, Writer: typeof(CharWriter)),
        [typeof(byte[])] = new(BinaryViewType.Default, Writer: typeof(BinaryWriter)),
        [typeof(Guid)] = new(BinaryViewType.Default, Writer: typeof(GuidWriter)),
    }.ToFrozenDictionary();

    private static Dictionary<string, string>? Metadata(Type core) => EnumCategories.TryGetArrowFieldMetadata(core);

    private static ScalarLayout? Scalar([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type type)
    {
        if (Scalars.TryGetValue(type, out ScalarLayout? scalar)) return scalar;
        if (!type.IsEnum) return null;
        Type underlying = Enum.GetUnderlyingType(type);
        if (!EnumCategories.UsesEnumDataType(type))
        {
            ScalarLayout integer = Scalars[underlying];
            return integer with { Converter = typeof(EnumToUnderlying<,>).MakeGenericType(type, underlying) };
        }
        Type writer = typeof(EnumWriter<>).MakeGenericType(type);
        return new ScalarLayout(new DictionaryType(UInt32Type.Default, StringViewType.Default, false), Writer: writer);
    }
}

// =====================================================================================================================
// Writers: each appends one slot per value to its builder (nested writers append to their children first).
// =====================================================================================================================

internal abstract class ArrowValueWriter(NativeColumnBuilder builder)
{
    public NativeColumnBuilder Builder { get; } = builder;
}

internal abstract class ArrowValueWriter<TValue>(NativeColumnBuilder builder) : ArrowValueWriter(builder)
{
    public abstract void Write(TValue value);

    /// <summary>Appends a run of values (the items of a list).</summary>
    public virtual void WriteAll(ReadOnlySpan<TValue> values)
    {
        foreach (TValue value in values) Write(value);
    }
}

/// <summary>Converts a .NET value to the fixed-width value Arrow stores.</summary>
internal interface IArrowValueConverter<TIn, TOut>
{
    static abstract TOut Convert(TIn value);
}

internal sealed class FixedWriter<TIn, TOut, TConv>(NativeColumnBuilder builder) : ArrowValueWriter<TIn>(builder)
    where TOut : unmanaged where TConv : struct, IArrowValueConverter<TIn, TOut>
{
    private readonly NativeFixedWidthBuilder<TOut> _values = (NativeFixedWidthBuilder<TOut>)builder;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Write(TIn value) => _values.Append(TConv.Convert(value));

    public override void WriteAll(ReadOnlySpan<TIn> values)
    {
        Span<TOut> slots = _values.AppendSlots(values.Length);
        for (int i = 0; i < values.Length; i++) slots[i] = TConv.Convert(values[i]);
    }
}

internal sealed class NullableFixedWriter<TIn, TOut, TConv>(NativeColumnBuilder builder) : ArrowValueWriter<TIn?>(builder)
    where TIn : struct where TOut : unmanaged where TConv : struct, IArrowValueConverter<TIn, TOut>
{
    private readonly NativeFixedWidthBuilder<TOut> _values = (NativeFixedWidthBuilder<TOut>)builder;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Write(TIn? value)
    {
        if (value.HasValue) _values.Append(TConv.Convert(value.GetValueOrDefault()));
        else _values.AppendNull();
    }
}

internal sealed class NullableWriter<TValue>(ArrowValueWriter<TValue> inner) : ArrowValueWriter<TValue?>(inner.Builder) where TValue : struct
{
    public override void Write(TValue? value)
    {
        if (value.HasValue) inner.Write(value.GetValueOrDefault());
        else Builder.AppendNull();
    }
}

internal sealed class OptionWriter<TValue>(ArrowValueWriter<TValue> inner) : ArrowValueWriter<FSharpOption<TValue>>(inner.Builder)
{
    public override void Write(FSharpOption<TValue> value)
    {
        if (value is null) Builder.AppendNull();
        else inner.Write(value.Value);
    }
}

internal sealed class ValueOptionWriter<TValue>(ArrowValueWriter<TValue> inner) : ArrowValueWriter<FSharpValueOption<TValue>>(inner.Builder)
{
    public override void Write(FSharpValueOption<TValue> value)
    {
        if (value.IsSome) inner.Write(value.Value);
        else Builder.AppendNull();
    }
}

internal sealed class BooleanWriter(NativeColumnBuilder builder) : ArrowValueWriter<bool>(builder)
{
    private readonly NativeBooleanBuilder _values = (NativeBooleanBuilder)builder;

    public override void Write(bool value) => _values.Append(value);
}

internal sealed class StringWriter(NativeColumnBuilder builder) : ArrowValueWriter<string?>(builder)
{
    private readonly NativeBinaryViewBuilder _values = (NativeBinaryViewBuilder)builder;

    public override void Write(string? value)
    {
        if (value is null) _values.AppendNull();
        else _values.AppendUtf8(value);
    }
}

internal sealed class CharWriter(NativeColumnBuilder builder) : ArrowValueWriter<char>(builder)
{
    private readonly NativeBinaryViewBuilder _values = (NativeBinaryViewBuilder)builder;

    public override void Write(char value) => _values.AppendUtf8(new ReadOnlySpan<char>(in value));
}

internal sealed class BinaryWriter(NativeColumnBuilder builder) : ArrowValueWriter<byte[]?>(builder)
{
    private readonly NativeBinaryViewBuilder _values = (NativeBinaryViewBuilder)builder;

    public override void Write(byte[]? value)
    {
        if (value is null) _values.AppendNull();
        else _values.Append(value);
    }
}

/// <summary>A <see cref="Guid"/> as its 16 bytes in .NET order (<see cref="Guid.TryWriteBytes(Span{byte})"/>).</summary>
internal sealed class GuidWriter(NativeColumnBuilder builder) : ArrowValueWriter<Guid>(builder)
{
    private readonly NativeBinaryViewBuilder _values = (NativeBinaryViewBuilder)builder;

    public override void Write(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes);
        _values.Append(bytes);
    }
}

/// <summary>A non-[Flags] enum as the position of its member name among the enum's categories.</summary>
internal sealed class EnumWriter<TEnum> : ArrowValueWriter<TEnum> where TEnum : struct, Enum
{
    private static readonly FrozenDictionary<TEnum, int> Positions = EnumCategories.GetUnderlyingValues(typeof(TEnum))
        .Select((v, i) => (Value: (TEnum)Enum.ToObject(typeof(TEnum), v), Position: i))
        .ToFrozenDictionary(p => p.Value, p => p.Position);

    private readonly NativeDictionaryBuilder _indices;

    public EnumWriter(NativeColumnBuilder builder) : base(builder)
    {
        _indices = (NativeDictionaryBuilder)builder;
        _indices.Symbols = EnumCategories.GetCategories(typeof(TEnum));
    }

    public override void Write(TEnum value)
    {
        if (!Positions.TryGetValue(value, out int position)) EnumCategories.GetCategory(typeof(TEnum), value); // throws: not a member
        _indices.Append(position);
    }
}

internal sealed class ListWriter<TCollection, TItem>(NativeColumnBuilder builder, ArrowValueWriter<TItem> items) : ArrowValueWriter<TCollection>(builder)
{
    private readonly NativeLargeListBuilder _lists = (NativeLargeListBuilder)builder;

    public override void Write(TCollection value)
    {
        switch (value)
        {
            case null:
                _lists.AppendNull();
                return;
            case TItem[] array:
                items.WriteAll(array);
                break;
            case List<TItem> list:
                items.WriteAll(CollectionsMarshal.AsSpan(list));
                break;
            default:
                foreach (TItem item in (IEnumerable<TItem>)value) items.Write(item);
                break;
        }
        _lists.EndList();
    }
}

internal sealed class StructWriter<TStruct>(NativeStructBuilder builder, ArrowValueWriter[] members, Action<TStruct, ArrowValueWriter[]> append)
    : ArrowValueWriter<TStruct>(builder)
{
    public override void Write(TStruct value)
    {
        if (value is null)
        {
            builder.AppendNull();
            return;
        }
        append(value, members);
        builder.EndStruct();
    }
}

internal sealed class ToStringWriter<TValue>(NativeColumnBuilder builder) : ArrowValueWriter<TValue>(builder)
{
    private readonly NativeBinaryViewBuilder _values = (NativeBinaryViewBuilder)builder;

    public override void Write(TValue value)
    {
        if ((value?.ToString()) is { } text) _values.AppendUtf8(text);
        else _values.AppendNull();
    }
}

// =====================================================================================================================
// Converters (struct generics: compiled into the writers, no calls).
// =====================================================================================================================

internal readonly struct Same<T> : IArrowValueConverter<T, T>
{
    public static T Convert(T value) => value;
}

internal readonly struct EnumToUnderlying<TEnum, TUnderlying> : IArrowValueConverter<TEnum, TUnderlying>
    where TEnum : struct, Enum where TUnderlying : unmanaged
{
    public static TUnderlying Convert(TEnum value) => Unsafe.As<TEnum, TUnderlying>(ref value);
}

/// <summary>Wall clock ticks since the Unix epoch, in microseconds (toward zero, as the engine converts them).</summary>
internal readonly struct DateTimeToMicros : IArrowValueConverter<DateTime, long>
{
    public static long Convert(DateTime value) => (value.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMicrosecond;
}

internal readonly struct DateTimeOffsetToMicros : IArrowValueConverter<DateTimeOffset, long>
{
    public static long Convert(DateTimeOffset value) => (value.UtcTicks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMicrosecond;
}

internal readonly struct TimeSpanToMicros : IArrowValueConverter<TimeSpan, long>
{
    public static long Convert(TimeSpan value) => value.Ticks / TimeSpan.TicksPerMicrosecond;
}

internal readonly struct TimeOnlyToNanos : IArrowValueConverter<TimeOnly, long>
{
    public static long Convert(TimeOnly value) => value.Ticks * 100;
}

internal readonly struct DateOnlyToDays : IArrowValueConverter<DateOnly, int>
{
    private static readonly int Epoch = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;

    public static int Convert(DateOnly value) => value.DayNumber - Epoch;
}

/// <summary>A decimal as decimal(38, 18): its value times 10^18, refused when that is not a whole number below 10^38.</summary>
internal readonly struct DecimalToScaled : IArrowValueConverter<decimal, Int128>
{
    private const int Scale = 18;
    private static readonly UInt128 Limit = UInt128.Parse("100000000000000000000000000000000000000"); // 10^38

    public static Int128 Convert(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        UInt128 mantissa = ((UInt128)(uint)bits[2] << 64) | ((ulong)(uint)bits[1] << 32) | (uint)bits[0];
        int scale = (bits[3] >> 16) & 0xFF;
        if (scale <= Scale)
        {
            UInt128 factor = Pow10(Scale - scale);
            if (mantissa > (Limit - 1) / factor) throw Unfit(value);
            mantissa *= factor;
        }
        else
        {
            UInt128 divisor = Pow10(scale - Scale);
            if (mantissa % divisor != 0) throw Unfit(value);
            mantissa /= divisor;
        }
        Int128 scaled = (Int128)mantissa;
        return bits[3] < 0 ? -scaled : scaled;
    }

    private static UInt128 Pow10(int exponent)
    {
        UInt128 result = 1;
        for (int i = 0; i < exponent; i++) result *= 10;
        return result;
    }

    private static OverflowException Unfit(decimal value) =>
        new($"The decimal {value} does not fit decimal(38, 18) exactly: at most 20 integer and 18 fractional digits (round it, e.g. Math.Round(value, 18)).");
}
