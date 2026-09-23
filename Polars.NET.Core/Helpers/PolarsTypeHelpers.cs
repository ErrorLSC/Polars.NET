namespace Polars.NET.Core.Helpers;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.FSharp.Core;
using Microsoft.FSharp.Reflection;

public enum OptionalKind
{
    None = 0,
    Nullable = 1,
    FSharpOption = 2,
    FSharpValueOption = 3
}

/// <summary>
/// Unified runtime type metadata and unwrapping helper for Polars.NET.
/// Directly integrates FSharp.Core to recognize records, unions, and optional wrappers without string-based reflection.
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

    private static readonly ConcurrentDictionary<Type, (Type CoreType, OptionalKind Kind)> UnwrapCache = new();

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
    /// Unwraps Nullable&lt;T&gt;, FSharpOption&lt;T&gt;, or FSharpValueOption&lt;T&gt; to its underlying payload type.
    /// </summary>
    public static Type UnwrapCoreType(Type type) => GetOptionalInfo(type).CoreType;

    /// <summary>
    /// Identifies the wrapping category and inner payload type of an optional container.
    /// </summary>
    public static (Type CoreType, OptionalKind Kind) GetOptionalInfo(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return UnwrapCache.GetOrAdd(type, static t =>
        {
            var nullableUnderlying = Nullable.GetUnderlyingType(t);
            if (nullableUnderlying != null)
                return (nullableUnderlying, OptionalKind.Nullable);

            if (t.IsGenericType)
            {
                var genericDef = t.GetGenericTypeDefinition();

                if (genericDef == typeof(FSharpOption<>))
                    return (t.GetGenericArguments()[0], OptionalKind.FSharpOption);

                if (genericDef == typeof(FSharpValueOption<>))
                    return (t.GetGenericArguments()[0], OptionalKind.FSharpValueOption);
            }

            return (t, OptionalKind.None);
        });
    }

    /// <summary>
    /// Checks if a type is an F# record type.
    /// </summary>
    public static bool IsFSharpRecord(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return FSharpType.IsRecord(type, null);
    }

    /// <summary>
    /// Retrieves column property schema for a type, leveraging FSharp.Reflection for F# records
    /// to guarantee field declaration order and handle internal representations cleanly.
    /// </summary>
    public static PropertyInfo[] GetModelProperties(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        if (IsFSharpRecord(type))
        {
            return FSharpType.GetRecordFields(type,null);
        }

        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var validProps = new List<PropertyInfo>(props.Length);

        foreach (var p in props)
        {
            if (p.CanRead && p.GetIndexParameters().Length == 0)
            {
                validProps.Add(p);
            }
        }

        return [.. validProps];
    }

    /// <summary>
    /// Checks if a type accepts null / none representations (Nullable, Option, ValueOption, or Reference Type).
    /// </summary>
    public static bool AcceptsNull(Type type)
    {
        if (!type.IsValueType) return true;
        var (_, kind) = GetOptionalInfo(type);
        return kind != OptionalKind.None;
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
    /// Builds an Expression that extracts the unwrapped value or returns default/null.
    /// Unwraps Nullable.Value, FSharpOption.Value, or FSharpValueOption.Value if present.
    /// </summary>
    public static Expression BuildUnwrapExpression(Expression propertyAccess, Type propertyType, Type targetType)
    {
        var (coreType, kind) = GetOptionalInfo(propertyType);

        switch (kind)
        {
            case OptionalKind.None:
                return propertyType != targetType ? Expression.Convert(propertyAccess, targetType) : propertyAccess;

            case OptionalKind.Nullable:
                // prop.HasValue ? (Target)prop.Value : default(Target)
                return Expression.Condition(
                    Expression.Property(propertyAccess, nameof(Nullable<int>.HasValue)),
                    Expression.Convert(Expression.Property(propertyAccess, nameof(Nullable<int>.Value)), targetType),
                    Expression.Default(targetType)
                );

            case OptionalKind.FSharpOption:
                // FSharpOption<T>.get_IsSome(prop) ? (Target)prop.Value : default(Target)
                var isSomeMethod = propertyType.GetProperty("IsSome", BindingFlags.Public | BindingFlags.Instance)!;
                var valueProp = propertyType.GetProperty("Value", BindingFlags.Public | BindingFlags.Instance)!;

                return Expression.Condition(
                    Expression.Property(propertyAccess, isSomeMethod),
                    Expression.Convert(Expression.Property(propertyAccess, valueProp), targetType),
                    Expression.Default(targetType)
                );

            case OptionalKind.FSharpValueOption:
                // prop.IsSome ? (Target)prop.Value : default(Target)
                var vOptionIsSome = propertyType.GetProperty("IsSome", BindingFlags.Public | BindingFlags.Instance)!;
                var vOptionValue = propertyType.GetProperty("Value", BindingFlags.Public | BindingFlags.Instance)!;

                return Expression.Condition(
                    Expression.Property(propertyAccess, vOptionIsSome),
                    Expression.Convert(Expression.Property(propertyAccess, vOptionValue), targetType),
                    Expression.Default(targetType)
                );

            default:
                return Expression.Convert(propertyAccess, targetType);
        }
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