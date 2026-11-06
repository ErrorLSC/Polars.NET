namespace Polars.NET.Core.Helpers;

using System;
using System.Collections.Concurrent;
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
            return FSharpType.GetRecordFields(type, null);
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
        if (AcceptsNull(type))
        {
            return type;
        }

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
    /// The payload of a boxed optional value (Nullable, F# option or value option), or null when it is absent.
    /// A value of a non-optional type (<see cref="OptionalKind.None"/>) is returned as is.
    /// </summary>
    public static object? UnwrapOptionalValue(object? boxed, OptionalKind optionalKind)
    {
        if (boxed is null)
        {
            return null;
        }

        switch (optionalKind)
        {
            case OptionalKind.FSharpOption:
            case OptionalKind.FSharpValueOption:
                var type = boxed.GetType();
                if (optionalKind == OptionalKind.FSharpValueOption
                    && !(bool)type.GetProperty("IsSome", BindingFlags.Public | BindingFlags.Instance)!.GetValue(boxed)!)
                {
                    return null;
                }
                return type.GetProperty("Value", BindingFlags.Public | BindingFlags.Instance)!.GetValue(boxed);
            default:
                return boxed;
        }
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

    /// <summary>
    /// Checks whether the specified type represents a supported numeric primitive or value
    /// (e.g. byte, int, long, float, double, decimal, Half, Int128, etc., including Nullable equivalents).
    /// </summary>
    public static bool IsNumeric(Type? type)
    {
        if (type == null) return false;
        Type core = UnwrapCoreType(type);
        return core == typeof(byte) || core == typeof(sbyte) ||
               core == typeof(short) || core == typeof(ushort) ||
               core == typeof(int) || core == typeof(uint) ||
               core == typeof(long) || core == typeof(ulong) ||
               core == typeof(Half) || core == typeof(float) ||
               core == typeof(double) || core == typeof(decimal) ||
               core == typeof(Int128) || core == typeof(UInt128);
    }

    /// <summary>
    /// Returns the zero-value literal object for a given numeric type, or null if not numeric.
    /// Unwraps Nullable&lt;T&gt;, FSharpOption&lt;T&gt;, and FSharpValueOption&lt;T&gt;.
    /// </summary>
    public static object? GetNumericZeroValue(Type? type)
    {
        if (type == null) return null;
        Type core = UnwrapCoreType(type);
        if (!IsNumeric(core)) return null;
        return Activator.CreateInstance(core);
    }

    /// <summary>
    /// Returns the default value for the specified type (Activator.CreateInstance for value types, null for reference types).
    /// </summary>
    public static object? GetDefaultValue(Type? type)
    {
        if (type == null) return null;
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>
    /// Checks whether the specified type represents a supported primitive or scalar value
    /// (e.g. primitives, string, decimal, DateOnly, DateTime, TimeSpan, etc., including Nullable<T> equivalents).
    /// </summary>
    public static bool IsScalarType(Type type)
    {
        if (type == null) return false;

        Type underlying = GetUnderlyingOrSelf(type);

        return underlying.IsPrimitive
            || underlying.IsEnum
            || underlying == typeof(string)
            || underlying == typeof(IEnumerable<char>)
            || underlying == typeof(decimal)
            || underlying == typeof(DateTime)
            || underlying == typeof(DateTimeOffset)
            || underlying == typeof(TimeSpan)
            || underlying == typeof(DateOnly)
            || underlying == typeof(TimeOnly)
            || underlying == typeof(Guid);
    }

    /// <summary>
    /// Checks whether the specified type represents a temporal date/time type (DateTime, DateTimeOffset, DateOnly, TimeOnly, TimeSpan).
    /// </summary>
    public static bool IsTemporalType(Type? type)
    {
        if (type == null) return false;
        Type core = UnwrapCoreType(type);
        return core == typeof(DateTime)
            || core == typeof(DateTimeOffset)
            || core == typeof(DateOnly)
            || core == typeof(TimeOnly)
            || core == typeof(TimeSpan);
    }
    /// <summary>
    /// Checks whether the specified type represents a composite Struct container
    /// (e.g. C# POCO/record struct or F# record) rather than a scalar or an enumerable collection.
    /// </summary>
    public static bool IsStructType(Type type)
    {
        if (type == null) return false;

        Type core = UnwrapCoreType(type);

        // 1. Scalar, primitive, or supported simple types are not structs
        if (IsScalarType(core) || IsSupportedSimpleType(core))
            return false;

        // 2. Collections and arrays are mapped to List columns
        if (TryGetEnumerableElementType(core) != null || typeof(System.Collections.IEnumerable).IsAssignableFrom(core))
            return false;

        // 3. Exclude System root types
        if (core == typeof(object) || core == typeof(ValueType))
            return false;

        // 4. Exclude tuples and anonymous types (used for Zip/Joins and transparent scopes)
        if (IsTupleOrAnonymousType(core))
            return false;

        // 5. Must be an F# record, class, or custom struct
        return IsFSharpRecord(core) || core.IsClass || (core.IsValueType && !core.IsPrimitive && !core.IsEnum);
    }

    /// <summary>
    /// Checks whether the specified type represents a compiler-generated or runtime tuple,
    /// anonymous type, or LINQ transparent identifier wrapper (e.g. System.Tuple, ValueTuple,
    /// F# AnonymousObject, Roslyn TransparentIdentifier, C# AnonymousType).
    /// </summary>
    public static bool IsTupleOrAnonymousType(Type? type)
    {
        if (type == null) return false;
        Type core = UnwrapCoreType(type);
        string? name = core.Name;
        if (name == null) return false;

        return name.StartsWith("AnonymousObject")
            || name.StartsWith("Tuple")
            || name.StartsWith("ValueTuple")
            || name.Contains("TransparentIdentifier")
            || name.Contains("AnonymousType");
    }

    /// <summary>
    /// Resolves the serialized column/field name for a member, checking [JsonPropertyName].
    /// </summary>
    public static string GetSerializedName(MemberInfo member)
    {
        var attr = member.GetCustomAttribute<System.Text.Json.Serialization.JsonPropertyNameAttribute>();
        return (!string.IsNullOrEmpty(attr?.Name)) ? attr.Name : member.Name;
    }

    /// <summary>
    /// Resolves the serialized column/field name for a parameter, checking [JsonPropertyName].
    /// </summary>
    public static string GetSerializedName(ParameterInfo parameter)
    {
        var attr = parameter.GetCustomAttribute<System.Text.Json.Serialization.JsonPropertyNameAttribute>();
        return (!string.IsNullOrEmpty(attr?.Name)) ? attr.Name : (parameter.Name?.TrimStart('@') ?? string.Empty);
    }
}