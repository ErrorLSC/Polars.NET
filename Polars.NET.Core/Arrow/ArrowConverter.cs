using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.FSharp.Core;

namespace Polars.NET.Core.Arrow;

public static class ArrowConverter
{
    // Cache Generic Type method definition to avoid reflection in every cycle
    private static readonly MethodInfo _buildMethodDef = typeof(ArrowConverter)
        .GetMethod("Build", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("ArrowConverter.Build<T> method not found.");

    // Cache F# Option Handler
    private static readonly MethodInfo _buildFSharpValOptMethod = typeof(ArrowConverter)
        .GetMethod(nameof(BuildFSharpOptionStruct), BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BuildFSharpOptionStruct method not found.");

    private static readonly MethodInfo _buildFSharpRefOptMethod = typeof(ArrowConverter)
        .GetMethod(nameof(BuildFSharpOptionClass), BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BuildFSharpOptionClass method not found.");

    private static class SchemaCache<T>
    {
        public static readonly Schema Default = GetSchemaFromType<T>();
    }

    /// <summary>
    /// Build Empty RecordBatch with Schema Only
    /// </summary>
    public static RecordBatch GetEmptyBatch<T>()
    {
        var schema = SchemaCache<T>.Default; 
        
        var emptyStruct = StructBuilderHelper.BuildStructArray(Enumerable.Empty<T>());
        
        return new RecordBatch(schema, emptyStruct.Fields, 0);
    }

    /// <summary>
    /// Convert any IEnumerable/Array into Arrow Array
    /// </summary>
    public static IArrowArray? BuildSingleColumn(object colValue)
    {
        if (colValue == null) return null;

        // Get Element type
        Type? elemType = ArrowTypeResolver.GetEnumerableElementType(colValue.GetType());
        
        if (elemType == null) 
            return null; 

        // Build Generic Method
        var buildMethod = _buildMethodDef.MakeGenericMethod(elemType);
        
        try 
        {
            // Call Apache.Arrow Build Method
            return (IArrowArray)buildMethod.Invoke(null, [colValue])!;
        }
        catch (TargetInvocationException ex) 
        { 
            throw ex.InnerException ?? ex; 
        }
    }

    /// <summary>
    /// General Entry：Decide which type of Arrary based on the type of T
    /// </summary>
    public static IArrowArray Build<T>(IEnumerable<T> data)
    {
        var type = typeof(T);

        // =====================================================================
        // 1. Array Interception (Must comes First!)
        // =====================================================================
        if (type.IsArray)
        {
            var elemType = type.GetElementType()!;
            var method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(elemType);
            return (IArrowArray)method.Invoke(null, [data])!;
        }

        // =====================================================================
        // 2. List Interception
        // =====================================================================
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            var elemType = type.GetGenericArguments()[0];
            var method = typeof(ArrowConverter)
                    .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(elemType);
                return (IArrowArray)method.Invoke(null, [data])!;
        }

        // 3. Unwrap F# Option type
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FSharpOption<>))
        {
            var innerType = type.GetGenericArguments()[0];

            if (innerType.IsValueType)
            {
                return (IArrowArray)_buildFSharpValOptMethod
                    .MakeGenericMethod(innerType)
                    .Invoke(null, [data])!;
            }
            else
            {
                return (IArrowArray)_buildFSharpRefOptMethod
                    .MakeGenericMethod(innerType)
                    .Invoke(null, [data])!;
            }
        }

        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        Type checkType = underlyingType ?? type;

        // 4. Primitives & String
        if (checkType == typeof(int)) return BuildInt32(data.Cast<int?>());
        if (checkType == typeof(uint)) return BuildUInt32(data.Cast<uint?>());
        if (checkType == typeof(string)) return BuildString(data.Cast<string?>());
        if (checkType == typeof(double)) return BuildDouble(data.Cast<double?>());
        if (checkType == typeof(bool)) return BuildBoolean(data.Cast<bool?>());
        if (checkType == typeof(byte)) return BuildUInt8(data.Cast<byte?>());
        if (checkType == typeof(sbyte)) return BuildInt8(data.Cast<sbyte?>());
        if (checkType == typeof(long)) return BuildInt64(data.Cast<long?>());
        if (checkType == typeof(ulong)) return BuildUInt64(data.Cast<ulong?>());
        if (checkType == typeof(short)) return BuildInt16(data.Cast<short?>());
        if (checkType == typeof(ushort)) return BuildUInt16(data.Cast<ushort?>());
        if (checkType == typeof(Half)) return BuildFloat16(data.Cast<Half?>());
        if (checkType == typeof(float)) return BuildFloat32(data.Cast<float?>());
        if (checkType == typeof(decimal)) return BuildDecimal(data.Cast<decimal?>());
        if (checkType == typeof(DateOnly)) return BuildDate32(data.Cast<DateOnly?>());
        if (checkType == typeof(TimeOnly)) return BuildTime64(data.Cast<TimeOnly?>());
        if (checkType == typeof(DateTime)) return BuildTimestamp(data.Cast<DateTime?>());
        if (checkType == typeof(DateTimeOffset)) return BuildDateTimeOffset(data.Cast<DateTimeOffset?>());
        if (checkType == typeof(TimeSpan)) return BuildDuration(data.Cast<TimeSpan?>());
        // if (checkType == typeof(Half)) return BuildFloat16(data.Cast<Half?>());
        var elementType = ArrowTypeResolver.GetEnumerableElementType(type);
        if (elementType != null)
        {
            var method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(elementType);

            return (IArrowArray)method.Invoke(null, [data])!;
        }
        // 5. Struct / Class / Object
        if (type.IsClass || type.IsValueType)
        {
            // [SMART RECOVERY] Handle Object[] that actually contains Structs
            if (checkType == typeof(object))
            {
                var dataList = data as IList<T> ?? data.ToList();
                var firstItem = dataList.FirstOrDefault(x => x != null);

                if (firstItem != null)
                {
                    Type runtimeType = firstItem.GetType();
                    // If runtime type is complex (Anonymous/Class) and NOT string
                    if (runtimeType.IsClass && runtimeType != typeof(string))
                    {
                        try
                        {
                            // Try to recover via StructBuilder
                            var method = typeof(StructBuilderHelper)
                                .GetMethod(nameof(StructBuilderHelper.BuildStructArray), BindingFlags.Public | BindingFlags.Static)!
                                .MakeGenericMethod(runtimeType);

                            // We need to Cast<RuntimeType>
                            var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast), BindingFlags.Public | BindingFlags.Static)!
                                .MakeGenericMethod(runtimeType);
                            var castData = castMethod.Invoke(null, [dataList]);

                            return (IArrowArray)method.Invoke(null, [castData!])!;
                        }
                        catch 
                        { 
                            // Fallback to String if recovery fails
                        }
                    }
                }

                // Default Object fallback: ToString
                var stringBuilder = new StringViewArray.Builder();
                foreach (var item in data)
                {
                    if (item == null) stringBuilder.AppendNull();
                    else stringBuilder.Append(item.ToString());
                }
                return stringBuilder.Build();
            }

            // Normal Struct Path
            try 
            {
                return StructBuilderHelper.BuildStructArray(data);
            }
            catch (Exception ex)
            {
                throw new NotSupportedException($"Type {type.FullName} (Underlying: {checkType.FullName}) is not supported yet.", ex);
            }
        }

        // 6. Fallback via Resolver (Recursive catch-all)

        throw new NotSupportedException($"Type {type.FullName} (Underlying: {checkType.FullName}) is not supported yet.");
    }

    // =================================================================
    // F# Direct Support (Split for Generic Constraints)
    // =================================================================
    
    /// <summary>
    /// Handler for Value Types: FSharpOption<int> -> int?
    /// Constraint: T must be struct
    /// </summary>
    private static IArrowArray BuildFSharpOptionStruct<T>(IEnumerable<FSharpOption<T>> data) 
        where T : struct
    {
        var nullableSeq = data.Select(opt => 
            opt == null ? (T?)null : opt.Value
        );
        return Build(nullableSeq);
    }

    /// <summary>
    /// Handler for Reference Types: FSharpOption<string> -> string
    /// Constraint: T must be class
    /// </summary>
    private static IArrowArray BuildFSharpOptionClass<T>(IEnumerable<FSharpOption<T>> data) 
        where T : class
    {
        var refSeq = data.Select(opt => opt?.Value);
        return Build(refSeq!);
    }

    /// <summary>
    /// Build ListArray 
    /// </summary>
    public static LargeListArray BuildListArray<U>(IEnumerable<IEnumerable<U>?> data)
    {
        // Recursion Logic: Flatten -> Build<U>
        // This handles List<Struct>, List<List<int>>, etc.
        var flattenedData = new List<U>();
        
        var offsetsBuilder = new Int64Array.Builder();
        var validityBuilder = new BooleanArray.Builder();
        
        long currentOffset = 0;
        offsetsBuilder.Append(0); 

        int nullCount = 0;

        foreach (var subList in data)
        {
            if (subList == null)
            {
                validityBuilder.Append(false);
                offsetsBuilder.Append(currentOffset);
                nullCount++;
            }
            else
            {
                validityBuilder.Append(true);
                
                int count = 0;
                foreach (var item in subList)
                {
                    flattenedData.Add(item);
                    count++;
                }
                
                currentOffset += count;
                offsetsBuilder.Append(currentOffset);
            }
        }

        // Recursive Call!
        IArrowArray valuesArray = Build(flattenedData);

        var offsetsArray = offsetsBuilder.Build();
        var validityArray = validityBuilder.Build();
        
        var listType = new LargeListType(valuesArray.Data.DataType);

        return new LargeListArray(
            listType,
            data.Count(),
            offsetsArray.ValueBuffer,
            valuesArray,
            validityArray.ValueBuffer,
            nullCount
        );
    }

    private static HalfFloatArray BuildFloat16(IEnumerable<Half?> data)
    {
        var b = new HalfFloatArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static FloatArray BuildFloat32(IEnumerable<float?> data)
    {
        var b = new FloatArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static Decimal128Array BuildDecimal(IEnumerable<decimal?> data)
    {
        var type = new Decimal128Type(38, 18); 
        var b = new Decimal128Array.Builder(type);
        foreach (var v in data)
        {
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        }
        return b.Build();
    }
    private static Int8Array BuildInt8(IEnumerable<sbyte?> data)
    {
        var b = new Int8Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static UInt8Array BuildUInt8(IEnumerable<byte?> data)
    {
        var b = new UInt8Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static Int16Array BuildInt16(IEnumerable<short?> data)
    {
        var b = new Int16Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static UInt16Array BuildUInt16(IEnumerable<ushort?> data)
    {
        var b = new UInt16Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }    

    private static Int32Array BuildInt32(IEnumerable<int?> data)
    {
        var b = new Int32Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    
    private static Int64Array BuildInt64(IEnumerable<long?> data)
    {
        var b = new Int64Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    private static UInt32Array BuildUInt32(IEnumerable<uint?> data)
    {
        var b = new UInt32Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    
    private static UInt64Array BuildUInt64(IEnumerable<ulong?> data)
    {
        var b = new UInt64Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    private static DoubleArray BuildDouble(IEnumerable<double?> data)
    {
        var b = new DoubleArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static BooleanArray BuildBoolean(IEnumerable<bool?> data)
    {
        var b = new BooleanArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static StringViewArray BuildString(IEnumerable<string?> data)
    {
        var b = new StringViewArray.Builder();
        foreach (var v in data) b.Append(v);
        return b.Build();
    }
    // DateOnly -> Date32 (Days since epoch)
    private static Date32Array BuildDate32(IEnumerable<DateOnly?> data)
    {
        var b = new Date32Array.Builder();
        foreach (var v in data)
        {
            if (v.HasValue) b.Append(v.Value.ToDateTime(TimeOnly.MinValue));
            else b.AppendNull();
        }
        return b.Build();
    }
    // TimeOnly -> Time64 (Nanoseconds)
    private static Time64Array BuildTime64(IEnumerable<TimeOnly?> data)
    {
        var b = new Time64Array.Builder(TimeUnit.Nanosecond); 
        
        foreach (var v in data)
        {
            if (v.HasValue) 
            {
                b.Append(v.Value.Ticks * 100L); 
            }
            else 
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }

    // DateTime -> Timestamp (Microsecond)
    private static TimestampArray BuildTimestamp(IEnumerable<DateTime?> data)
    {
        var b = new TimestampArray.Builder(TimeUnit.Microsecond);
        
        foreach (var v in data)
        {
            if (v.HasValue)
            {
                DateTime dt = v.Value;
                
                var dto = new DateTimeOffset(dt.Ticks, TimeSpan.Zero);

                // Ticks (100ns) -> Microsecond (1000ns)
                b.Append(dto);
            }
            else 
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }
    private static TimestampArray BuildDateTimeOffset(IEnumerable<DateTimeOffset?> data)
    {
        var b = new TimestampArray.Builder(TimeUnit.Microsecond);
        
        foreach (var v in data)
        {
            if (v.HasValue)
            {
                b.Append(v.Value); 
            }
            else
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }
    private static DurationArray BuildDuration(IEnumerable<TimeSpan?> data)
    {
        var b = new DurationArray.Builder(DurationType.Microsecond);

        foreach (var v in data)
        {
            if (v.HasValue)
            {
                // Ticks (100ns) -> Microseconds (1000ns)
                b.Append(v.Value.Ticks / 10L);
            }
            else
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }

    public static Schema GetSchemaFromType<T>()
    {
        var type = typeof(T);
        var members = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var fields = new List<Field>();

        foreach (var member in members)
        {
            var memberType = member.PropertyType;
            var dummyInstance = CreateDummyInstance(memberType);
            
            var wrapper = System.Array.CreateInstance(memberType, 1);
            if (dummyInstance != null) wrapper.SetValue(dummyInstance, 0);
            
            var dummyArr = BuildSingleColumn(wrapper);
            if (dummyArr == null) continue;
            
            var field = new Field(member.Name, dummyArr.Data.DataType, true);
            fields.Add(field);
        }
        return new Schema(fields, null);
    }

    private static object? CreateDummyInstance(Type t)
    {
        if (t == typeof(string)) return string.Empty;
        if (t.IsArray) return System.Array.CreateInstance(t.GetElementType()!, 0);
        
        // C# List
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>))
        {
             return Activator.CreateInstance(t);
        }

        // F# List (Handling missing constructor)
        if (t.FullName != null && (t.FullName.StartsWith("Microsoft.FSharp.Collections.FSharpList") || t.Name == "FSharpList`1"))
        {
            var emptyProp = t.GetProperty("Empty", BindingFlags.Public | BindingFlags.Static);
            return emptyProp?.GetValue(null);
        }

        // F# Option
        if (t.FullName != null && t.FullName.StartsWith("Microsoft.FSharp.Core.FSharpOption")) return null;

        if (t.IsValueType) return Activator.CreateInstance(t);
        
        try 
        {
            return Activator.CreateInstance(t);
        }
        catch 
        {
            try 
            {
                return RuntimeHelpers.GetUninitializedObject(t);
            }
            catch 
            {
                return null; 
            }
        }
    }
    private static object? GetDefault(Type t) => t.IsValueType ? Activator.CreateInstance(t) : null;

    /// <summary>
    /// Slice IEnumerable<RecordBatch> to chuncks and convert it to ArrowBatchs
    /// </summary>
    public static IEnumerable<RecordBatch> ToArrowBatches<T>(IEnumerable<T> data, int batchSize)
    {
        var dummyStruct = StructBuilderHelper.BuildStructArray(Enumerable.Empty<T>());

        var structType = (StructType)dummyStruct.Data.DataType;
        var schema = new Schema(structType.Fields, null);
        
        bool hasYielded = false;

        foreach (var chunk in data.Chunk(batchSize))
        {
            hasYielded = true;
            var structArray = StructBuilderHelper.BuildStructArray(chunk);

            yield return new RecordBatch(schema, structArray.Fields, chunk.Length);
        }
        if (!hasYielded)
        {
            yield return new RecordBatch(schema, dummyStruct.Fields, 0);
        }
    }

    internal static class StructBuilderHelper
    {
        // =================================================================
        // Columnar Construction
        // =================================================================
        public static StructArray BuildStructArray<T>(IEnumerable<T> data)
        {
            var dataList = data as IList<T> ?? data.ToList();
            int length = dataList.Count;
            var type = typeof(T);

            // Direct Reflection to ensure exact names and internal props
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
            var fields = new List<Field>();
            var childrenArrays = new List<IArrowArray>();

            foreach (var prop in properties)
            {
                var memberType = prop.PropertyType;
                var getter = CompileGetter<T>(prop);
                
                // Recursively build column
                var childArray = ProjectAndBuild(dataList, memberType, getter);

                // [FIX] Use prop.Name directly (Case Sensitive)
                var finalField = new Field(prop.Name, childArray.Data.DataType, true);

                fields.Add(finalField);
                childrenArrays.Add(childArray);
            }

            // Build Validity Bitmap
            var validityBuilder = new ArrowBuffer.BitmapBuilder();
            int nullCount = 0;
            foreach (var item in dataList)
            {
                if (item == null) { validityBuilder.Append(false); nullCount++; }
                else { validityBuilder.Append(true); }
            }
            
            // [FIX] Removed .ValueBuffer
            var validityBuffer = validityBuilder.Build();

            var structType = new StructType(fields);
            
            return new StructArray(
                structType,
                length,
                childrenArrays,
                validityBuffer,
                nullCount
            );
        }

        // =================================================================
        // Helper：Reflection Bridge
        // =================================================================
        
        private static IArrowArray ProjectAndBuild<TParent>(IList<TParent> data, Type propType, Func<TParent, object?> getter)
        {
            Type cleanType = Nullable.GetUnderlyingType(propType) ?? propType;
            Type targetType = cleanType.IsValueType ? typeof(Nullable<>).MakeGenericType(cleanType) : cleanType;

            // F# Option Special Handling
            if (propType.FullName != null && propType.FullName.StartsWith("Microsoft.FSharp.Core.FSharpOption"))
                targetType = propType;

            var method = typeof(StructBuilderHelper)
                .GetMethod(nameof(BuildColumn), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TParent), targetType);
                
            return (IArrowArray)method.Invoke(null, [data, getter])!;
        }

        private static IArrowArray BuildColumn<TParent, TProp>(IList<TParent> data, Func<TParent, object?> getter)
        {
            var columnData = new List<TProp>(data.Count);
            foreach(var item in data)
            {
                if (item == null) { columnData.Add(default!); continue; }
                var rawVal = getter(item);
                if (rawVal == null) columnData.Add(default!);
                else columnData.Add((TProp)rawVal);
            }

            // Call main Build with strong type TProp
            return Build(columnData);
        }

        // =================================================================
        // Compile Expression Tree for Getter
        // =================================================================
        private static Func<T, object?> CompileGetter<T>(PropertyInfo prop)
        {
            var instanceParam = Expression.Parameter(typeof(T), "item");
            var memberAccess = Expression.Property(instanceParam, prop);
            var convertToObject = Expression.Convert(memberAccess, typeof(object));
            return Expression.Lambda<Func<T, object?>>(convertToObject, instanceParam).Compile();
        }
    }
}