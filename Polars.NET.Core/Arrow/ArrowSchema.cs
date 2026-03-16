using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.FSharp.Core; 

namespace Polars.NET.Core.Arrow;

public static class ArrowTypeResolver
{
    // =================================================================================
    // 1. DataReader -> Arrow Schema (Entry Point)
    // =================================================================================
    public static Schema GetSchemaFromDataReader(IDataReader reader)
    {
        var fields = new List<Field>();
        
        // Console.WriteLine($"[Resolver] Generating Schema for {reader.FieldCount} fields...");

        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var netType = reader.GetFieldType(i);
            
            // 1. Resolve Arrow Type 
            var arrowType = GetArrowTypeFromNetType(netType);
            
            // 2. Create Field
            var field = new Field(name, arrowType, nullable: true);
            
            // 3. Add 
            fields.Add(field);
        }

        if (fields.Count != reader.FieldCount)
        {
            throw new InvalidOperationException($"[Resolver Bug] Reader has {reader.FieldCount} fields but generated {fields.Count} schema fields.");
        }

        return new Schema(fields, null);
    }

    // =================================================================================
    // 2. .NET Type -> Arrow Type
    // =================================================================================
    public static IArrowType GetArrowTypeFromNetType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
        Type type)
    {
        // Handle Nullable<T> and F# Option
        bool isFSharpOption = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FSharpOption<>);
        Type coreType;
        
        if (isFSharpOption) coreType = type.GetGenericArguments()[0];
        else coreType = Nullable.GetUnderlyingType(type) ?? type;

        // 1. Primitives
        if (coreType == typeof(sbyte))  return Int8Type.Default;
        if (coreType == typeof(short))  return Int16Type.Default;
        if (coreType == typeof(int))    return Int32Type.Default;
        if (coreType == typeof(long))   return Int64Type.Default;
        if (coreType == typeof(byte))   return UInt8Type.Default;
        if (coreType == typeof(ushort)) return UInt16Type.Default;
        if (coreType == typeof(uint))   return UInt32Type.Default;
        if (coreType == typeof(ulong))  return UInt64Type.Default;
        if (coreType == typeof(Half))  return  HalfFloatType.Default;
        if (coreType == typeof(float))  return FloatType.Default;
        if (coreType == typeof(double)) return DoubleType.Default;
        if (coreType == typeof(bool))   return BooleanType.Default;
        if (coreType == typeof(decimal)) return new Decimal128Type(38, 18); 

        // 2. String & Binary
        if (coreType == typeof(string)) return StringViewType.Default;
        if (coreType == typeof(char))   return StringViewType.Default;
        if (coreType == typeof(Guid)) return BinaryViewType.Default;

        if (coreType == typeof(byte[])) return BinaryViewType.Default;

        // 3. Date & Time
        // TimeOnly -> Time64 (Nanosecond) [Polars Native]
        if (coreType == typeof(TimeOnly)) return new Time64Type(TimeUnit.Nanosecond);
        if (coreType == typeof(TimeSpan)) return DurationType.Microsecond; 
        
        if (coreType == typeof(DateOnly)) return Date32Type.Default;
        if (coreType == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, null as string);
        if (coreType == typeof(DateTimeOffset)) return new TimestampType(TimeUnit.Microsecond, "UTC");

        // 4. Complex Types (Recursive)
        
        // List / Array
        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(coreType) && coreType != typeof(string))
        {
            Type? elementType = GetEnumerableElementType(coreType);
            if (elementType != null)
            {
                var innerField = ResolveField("item", elementType);
                return new LargeListType(innerField);
            }
        }

        // Struct / Class (Reflection for UserMeta, etc.)
        if (!coreType.IsPrimitive && !coreType.IsEnum && coreType != typeof(object))
        {
            var members = GetReadableMembers(coreType);
            if (members.Length > 0)
            {
                // Recursively resolve members
                var fields = members.Select(m => ResolveField(m.Name, GetMemberType(m))).ToList();
                return new StructType(fields);
            }
        }

        // Fallback
        return StringViewType.Default;
    }

    // =================================================================================
    // 3. Helpers (Reflection & Struct Support)
    // =================================================================================
    
    public static Field ResolveField(string name, 
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    Type type)
    {
        bool isFSharpOption = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FSharpOption<>);
        bool isFSharpVOption = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FSharpValueOption<>); 
        
        bool isNullable = !type.IsValueType || Nullable.GetUnderlyingType(type) != null || isFSharpOption || isFSharpVOption;

        IArrowType arrowType = GetArrowTypeFromNetType(type); 

        return new Field(name, arrowType, isNullable);
    }

    public static MemberInfo[] GetReadableMembers(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
        Type type)
    {
        var flags = BindingFlags.Public | BindingFlags.Instance;
        
        var properties = type.GetProperties(flags)
            .Where(p => p.GetIndexParameters().Length == 0)
            .Where(p => !p.PropertyType.IsInterface && !p.PropertyType.IsAbstract)
            .Cast<MemberInfo>();

        var fields = type.GetFields(flags)
            .Where(f => !f.FieldType.IsInterface && !f.FieldType.IsAbstract)
            .Cast<MemberInfo>();

        return [.. properties, .. fields];
    }

    public static Type GetMemberType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
        MemberInfo member)
    {
        return member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new NotSupportedException($"Member {member.Name} is not a Property or Field.")
        };
    }

    public static Type? GetEnumerableElementType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
        Type type)
    {
        if (type.IsArray) return type.GetElementType();
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)) return type.GetGenericArguments()[0];
        var ienum = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        return ienum?.GetGenericArguments()[0];
    }

    // =================================================================================
    // 4. Reverse Mapping (Arrow -> .NET) - Keep this for ArrowToDbStream
    // =================================================================================
    public static Type GetNetTypeFromArrowType(IArrowType arrowType)
    {
        return arrowType switch
        {
            ListType list => GetNetTypeFromArrowType(list.ValueDataType).MakeArrayType(),
            LargeListType largeList => GetNetTypeFromArrowType(largeList.ValueDataType).MakeArrayType(),
            Int8Type => typeof(sbyte),
            Int16Type => typeof(short),
            Int32Type => typeof(int),
            Int64Type => typeof(long),
            UInt8Type => typeof(byte),
            UInt16Type => typeof(ushort),
            UInt32Type => typeof(uint),
            UInt64Type => typeof(ulong),
            HalfFloatType => typeof(Half),
            FloatType => typeof(float),
            DoubleType => typeof(double),
            BooleanType => typeof(bool),
            Decimal128Type => typeof(decimal),
            Decimal256Type => typeof(decimal),
            StringType => typeof(string),
            LargeStringType => typeof(string),
            StringViewType => typeof(string),
            TimestampType ts => string.IsNullOrEmpty(ts.Timezone) ? typeof(DateTime) : typeof(DateTimeOffset),
            Date32Type => typeof(DateOnly),
            Date64Type => typeof(DateTime),
            Time64Type => typeof(TimeOnly), 
            DurationType => typeof(TimeSpan),
            BinaryType => typeof(byte[]),
            LargeBinaryType => typeof(byte[]),
            BinaryViewType => typeof(byte[]),
            FixedSizeBinaryType => typeof(Guid),
            _ => typeof(object)
        };
    }
}
