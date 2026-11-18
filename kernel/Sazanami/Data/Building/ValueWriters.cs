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

namespace Kaminari.Sazanami.Data.Building;

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
