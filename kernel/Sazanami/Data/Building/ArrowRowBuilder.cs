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
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Sazanami.Data.Mapping;

namespace Kaminari.Sazanami.Data.Building;

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
/// readable properties) is written as its <see cref="object.ToString"/>. Value types are non-nullable columns; reference
/// types, <see cref="Nullable{T}"/>, F# options, list items and every member of a nested record nullable ones.
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

    /// <summary>
    /// The columns holding dictionaries (lists of Key / Value structs), by path: struct members joined by '.', a list's
    /// item as <c>[]</c>, a map's key and value as <c>.key</c> / <c>.value</c> (as the Parquet writer names columns).
    /// </summary>
    public static IReadOnlySet<string> MapColumns => Plan.Value.MapColumns;

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

        public IReadOnlySet<string> MapColumns { get; } =
            columns.SelectMany((c, i) => RowLayout.MapPaths(c, schema.FieldsList[i].Name)).ToFrozenSet();

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

/// <summary>
/// Builds one Arrow array of values of <typeparamref name="T"/> in native memory, laid out as
/// <see cref="ArrowRowBuilder{T}"/> lays out a column of that type (a record as a struct).
/// </summary>
public sealed class ArrowColumnBuilder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable
{
    private readonly RowNode _node = RowLayout.Resolve(typeof(T));
    private readonly NativeColumnBuilder _builder;
    private readonly ArrowValueWriter<T> _writer;

    public ArrowColumnBuilder()
    {
        _builder = NativeColumnBuilder.Create(_node.Type);
        _writer = (ArrowValueWriter<T>)_node.CreateWriter(_builder);
    }

    /// <summary>The field of the values (its type, nullability and metadata) under <paramref name="name"/>.</summary>
    public Field Field(string name) => _node.Field(name);

    /// <summary>The values appended since the last export.</summary>
    public long Count => _builder.Length;

    /// <summary>Appends a value (null where <typeparamref name="T"/> takes one).</summary>
    public void Append(T value) => _writer.Write(value);

    /// <summary>Appends a null slot (for values given as objects, where a null stands for any type's absent value).</summary>
    public void AppendNull() => _builder.AppendNull();

    /// <summary>The values appended since the last export, as an array over native memory; the builder starts empty again.</summary>
    public IArrowArray ExportArray() => _builder.ExportArray();

    public void Dispose() => _builder.Dispose();
}

/// <summary>
/// 128-bit integers, which Arrow (and Apache.Arrow) has no type for, as decimal(38, 0) columns marked by field metadata:
/// a decimal128 holds the same 16 little-endian two's complement bytes, so a consumer with 128-bit integers reads them
/// as such (the engine does), and any other one as exact decimals while they have at most 38 digits (Parquet: values of
/// 39 digits are refused; unsigned ones above 2^127 too). Once Apache.Arrow has 128-bit integer types, this layout will
/// change to them.
/// </summary>
public static class WideIntegers
{
    /// <summary>The Arrow type of a 128-bit integer column.</summary>
    public static Decimal128Type Type { get; } = new(38, 0);

    /// <summary>The kind (<see cref="Signed"/>, <see cref="Unsigned"/>) of a 128-bit integer field, or null.</summary>
    public static string? KindOf(Field field) =>
        field.DataType is Decimal128Type { Scale: 0 } ? field.Metadata?.GetValueOrDefault(MetadataKey) : null;

    /// <summary>The field metadata key marking a 128-bit integer column; its value is <see cref="Signed"/> or <see cref="Unsigned"/>.</summary>
    public const string MetadataKey = "kaminari:int128";

    public const string Signed = "i128";

    public const string Unsigned = "u128";
}
