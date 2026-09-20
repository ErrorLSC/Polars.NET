namespace Polars.NET.Core.Helpers;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

/// <summary>
/// Unified runtime type metadata and unwrapping helper for Polars.NET.
/// Bridges .NET primitives, structs, and F# types (Option, ValueOption, List) into Polars columnar formats.
/// </summary>
public static class PolarsTypeHelper
{
    private static readonly HashSet<Type> SimpleTypes =
    [
        typeof(bool), typeof(byte), typeof(sbyte),
        typeof(short), typeof(ushort), typeof(int), typeof(uint),
        typeof(long), typeof(ulong), typeof(float), typeof(double),
        typeof(decimal), typeof(string), typeof(DateTime), typeof(DateOnly),
        typeof(TimeOnly), typeof(TimeSpan), typeof(DateTimeOffset),
        typeof(Guid), typeof(Half), typeof(Int128), typeof(UInt128)
    ];

    /// <summary>
    /// Checks whether the specified type can be transposed via zero-copy / contiguous columnar buffers.
    /// Unwraps Nullable&lt;T&gt;, FSharpOption&lt;T&gt;, and FSharpValueOption&lt;T&gt;.
    /// </summary>
    public static bool IsSupportedSimpleType(Type type)
    {
        Type core = UnwrapCoreType(type);
        return core.IsPrimitive || core.IsEnum || SimpleTypes.Contains(core);
    }

    /// <summary>
    /// Unwraps Nullable&lt;T&gt;, FSharpOption&lt;T&gt;, or FSharpValueOption&lt;T&gt; to its inner payload type.
    /// </summary>
    public static Type UnwrapCoreType(Type type)
    {
        if (type == null) return typeof(object);

        var underlyingNullable = Nullable.GetUnderlyingType(type);
        if (underlyingNullable != null)
            return underlyingNullable;

        if (type.IsGenericType)
        {
            var def = type.GetGenericTypeDefinition();
            var name = def.FullName;

            // FSharpOption<'T> or FSharpValueOption<'T>
            if (name == "Microsoft.FSharp.Core.FSharpOption`1" ||
                name == "Microsoft.FSharp.Core.FSharpValueOption`1")
            {
                return type.GetGenericArguments()[0];
            }
        }

        return type;
    }

    /// <summary>
    /// Checks if a type accepts null / none representations (Nullable, Option, ValueOption, or Reference Type).
    /// </summary>
    public static bool AcceptsNull(Type type)
    {
        if (!type.IsValueType) return true;
        if (Nullable.GetUnderlyingType(type) != null) return true;

        if (type.IsGenericType)
        {
            var name = type.GetGenericTypeDefinition().FullName;
            if (name == "Microsoft.FSharp.Core.FSharpValueOption`1" ||
                name == "Microsoft.FSharp.Core.FSharpOption`1")
            {
                return true;
            }
        }

        return false;
    }

    public static Type GetUnderlyingOrSelf(Type type) => Nullable.GetUnderlyingType(type) ?? type;

    public static Type EnsureNullable(Type type)
    {
        return (type.IsValueType && Nullable.GetUnderlyingType(type) == null)
            ? typeof(Nullable<>).MakeGenericType(type)
            : type;
    }
    public static Type? TryGetEnumerableElementType(Type type)
    {
        if (type.IsArray) return type.GetElementType();

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            return type.GetGenericArguments()[0];

        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return iface.GetGenericArguments()[0];
        }

        return null;
    }
    /// <summary>
    /// Promotes two scalar types to their common denominator for DataFrame schema inference.
    /// </summary>
    public static Type PromoteType(Type typeA, Type typeB)
    {
        if (typeA == typeB) return typeA;

        Type baseA = UnwrapCoreType(typeA);
        Type baseB = UnwrapCoreType(typeB);

        if (baseA == baseB) return baseA;

        if (IsNumeric(baseA) && IsNumeric(baseB))
        {
            if (baseA == typeof(double) || baseB == typeof(double)) return typeof(double);
            if (baseA == typeof(float) || baseB == typeof(float)) return typeof(double);
            if (baseA == typeof(decimal) || baseB == typeof(decimal)) return typeof(decimal);
            if (baseA == typeof(long) || baseB == typeof(long)) return typeof(long);
            if (baseA == typeof(ulong) || baseB == typeof(ulong)) return typeof(ulong);
            return typeof(long);
        }

        return typeof(string);
    }

    private static bool IsNumeric(Type type)
    {
        return type == typeof(byte) || type == typeof(sbyte) ||
               type == typeof(short) || type == typeof(ushort) ||
               type == typeof(int) || type == typeof(uint) ||
               type == typeof(long) || type == typeof(ulong) ||
               type == typeof(Half) || type == typeof(float) ||
               type == typeof(double) || type == typeof(decimal) ||
               type == typeof(Int128) || type == typeof(UInt128);
    }
}
