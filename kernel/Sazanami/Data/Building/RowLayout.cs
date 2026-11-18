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

/// <summary>How one .NET type is laid out: its Arrow type, and how to make a writer over a builder of that type.</summary>
internal sealed class RowNode(IArrowType type, bool nullable, Dictionary<string, string>? metadata, Type writerType,
    Func<NativeColumnBuilder, ArrowValueWriter> createWriter, RowNode[]? children = null, bool isMap = false)
{
    /// <summary>The nodes inside: a struct's members, a list's item.</summary>
    public RowNode[] Children { get; } = children ?? [];

    /// <summary>Whether this list holds a dictionary's entries (a list of Key / Value structs).</summary>
    public bool IsMap { get; } = isMap;

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
        // A LINQ group is its key (a record of one member), not the sequence of its elements.
        if (Grouping(type) is { } grouping) return [grouping.GetProperty(nameof(IGrouping<int, int>.Key))!];
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
                return new RowNode(inner.Type, true, metadata, nullableFixed, b => (ArrowValueWriter)Activator.CreateInstance(nullableFixed, b)!,
                    inner.Children, inner.IsMap);
            }
            Type wrapper = (kind switch
            {
                OptionalKind.Nullable => typeof(NullableWriter<>),
                OptionalKind.FSharpOption => typeof(OptionWriter<>),
                _ => typeof(ValueOptionWriter<>),
            }).MakeGenericType(core);
            return new RowNode(inner.Type, true, metadata, wrapper, b => (ArrowValueWriter)Activator.CreateInstance(wrapper, inner.CreateWriter(b))!,
                inner.Children, inner.IsMap);
        }

        bool nullable = !type.IsValueType;
        if (Scalar(type) is { } scalar)
        {
            Type writer = scalar.Converter is { } conv
                ? typeof(FixedWriter<,,>).MakeGenericType(type, scalar.Stored!, conv)
                : scalar.Writer!;
            return new RowNode(scalar.Type, nullable, Metadata(type), writer, b => (ArrowValueWriter)Activator.CreateInstance(writer, b)!);
        }

        if (Grouping(type) is null && ElementType(type) is { } element)
        {
            RowNode item = Resolve(element);
            Type writer = typeof(ListWriter<,>).MakeGenericType(type, element);
            // List items are nullable fields, whatever their type: the engine's list types carry the item's nullability,
            // and its own lists' items are always nullable.
            return new RowNode(new LargeListType(new Field("item", item.Type, true, item.Metadata)), nullable, null, writer,
                b => (ArrowValueWriter)Activator.CreateInstance(writer, b, item.CreateWriter(((NativeLargeListBuilder)b).Values))!,
                [item], IsDictionary(type, element));
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
            // Members of a nested record are nullable fields: a null record's members are null (as the engine holds them).
            var structType = new StructType([.. members.Select((m, i) => new Field(m.Name, nodes[i].Type, true, nodes[i].Metadata))]);
            return new RowNode(structType, nullable, null, writer, b =>
            {
                var builder = (NativeStructBuilder)b;
                ArrowValueWriter[] children = [.. nodes.Select((n, i) => n.CreateWriter(builder.Children[i]))];
                return (ArrowValueWriter)Activator.CreateInstance(writer, builder, children, append)!;
            }, nodes);
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

    /// <summary>The <see cref="IGrouping{TKey, TElement}"/> interface <paramref name="type"/> is or implements, or null.</summary>
    private static Type? Grouping([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type type) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IGrouping<,>)
            ? type
            : type.GetInterfaces().FirstOrDefault(static i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IGrouping<,>));

    /// <summary>Whether a collection of <paramref name="element"/> is a dictionary (its entries: key / value pairs).</summary>
    private static bool IsDictionary([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type type, Type element) =>
        element.IsGenericType && element.GetGenericTypeDefinition() == typeof(KeyValuePair<,>)
        && (type.IsInterface ? [type, .. type.GetInterfaces()] : type.GetInterfaces()).Any(static i => i.IsGenericType
            && (i.GetGenericTypeDefinition() == typeof(IDictionary<,>) || i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)));

    /// <summary>
    /// The paths of the dictionary columns under <paramref name="node"/> at <paramref name="path"/>, as the Parquet writer
    /// names columns: struct members joined by '.', a list's item as <c>[]</c>, a map's key and value as <c>.key</c> /
    /// <c>.value</c>.
    /// </summary>
    public static IEnumerable<string> MapPaths(RowNode node, string path)
    {
        if (node.IsMap)
        {
            yield return path;
            RowNode entry = node.Children[0];
            foreach (string inner in MapPaths(entry.Children[0], path + ".key")) yield return inner;
            foreach (string inner in MapPaths(entry.Children[1], path + ".value")) yield return inner;
            yield break;
        }
        if (node.Type is LargeListType)
        {
            foreach (string inner in MapPaths(node.Children[0], path + "[]")) yield return inner;
            yield break;
        }
        if (node.Type is StructType structType)
        {
            for (int i = 0; i < node.Children.Length; i++)
                foreach (string inner in MapPaths(node.Children[i], $"{path}.{structType.Fields[i].Name}")) yield return inner;
        }
    }

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
        [typeof(Int128)] = new(WideIntegers.Type, typeof(Int128), typeof(Same<Int128>)),
        [typeof(UInt128)] = new(WideIntegers.Type, typeof(Int128), typeof(UInt128Bits)),
    }.ToFrozenDictionary();

    private static Dictionary<string, string>? Metadata(Type core) =>
        core == typeof(Int128) ? new() { [WideIntegers.MetadataKey] = WideIntegers.Signed }
        : core == typeof(UInt128) ? new() { [WideIntegers.MetadataKey] = WideIntegers.Unsigned }
        : EnumCategories.TryGetArrowFieldMetadata(core);

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
