using System.Collections;
using System.Reflection;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace Polars.NET.Core.Arrow
{
    public static class ArrowReader
    {
        public static IEnumerable<T> ReadRecordBatch<T>(RecordBatch batch)
        {
            
            var targetType = typeof(T);

            // Scalar Mode
            if (IsScalarType(targetType))
            {
                if (batch.ColumnCount == 0) yield break;

                var col = batch.Column(0);
                var accessor = CreateAccessor(col, targetType);
                int count = batch.Length;

                for (int i = 0; i < count; i++)
                {
                    var val = accessor(i);
                    yield return val == null ? default! : (T)val;
                }
                yield break;
            }
            // Object Mapping Mode
            // For POCO (class/struct)
            
            int rowCount = batch.Length;
            
            // Get Can-write properties
            var properties = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                       .Where(p => p.CanWrite).ToArray();
            
            var columnAccessors = new Func<int, object?>[properties.Length];

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                var col = batch.Column(prop.Name); 

                if (col == null) 
                {
                    columnAccessors[i] = _ => null; 
                    continue; 
                }

                columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
            }
        //     for (int i = 0; i < properties.Length; i++)
        //         {
        //             var prop = properties[i];
                    
        //             // 1. 安全查找列索引（忽略大小写，适应 SQL 的多变性）
        //             int colIndex = -1;
        //             for (int c = 0; c < batch.ColumnCount; c++)
        //             {
        //                 var field = batch.Schema.GetFieldByIndex(c);
        //                 if (string.Equals(field.Name, prop.Name, StringComparison.OrdinalIgnoreCase))
        //                 {
        //                     colIndex = c;
        //                     break;
        //                 }
        //             }

        //             // 2. 如果没找到对应的列，优雅回退到返回 null
        //             if (colIndex == -1) 
        //             {
        //                 columnAccessors[i] = _ => null; 
        //                 continue; 
        //             }

        //             // 3. 安全读取列数据
        //             var col = batch.Column(colIndex);
        //             columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
        //         }

            for (int i = 0; i < rowCount; i++)
            {
                var item = Activator.CreateInstance<T>()!; 
                
                for (int p = 0; p < properties.Length; p++)
                {
                    var accessor = columnAccessors[p];
                    var val = accessor(i);
                    if (val != null) properties[p].SetValue(item, val);
                }
                yield return item;
            }
        }

        // =============================================================
        // Accessor Factory
        // =============================================================
        public static Func<int, object?> CreateAccessor(IArrowArray array, Type targetType)
        {
            if (targetType == typeof(object))
            {
                targetType = ArrowTypeResolver.GetNetTypeFromArrowType(array.Data.DataType);
            }

            // Direct check for FSharpOption<U>
            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(FSharpOption<>))
            {
                var innerType = targetType.GetGenericArguments()[0];
                
                // Use Generic helper to construct specialized accessor
                var method = typeof(ArrowReader)
                    .GetMethod(nameof(CreateFSharpOptionAccessor), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(innerType);
                
                return (Func<int, object?>)method.Invoke(null, [array])!;
            }

            Type underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            return CreateCoreAccessor(array, underlyingType);
        }
        // Specialized F# Option Accessor Builder
        private static Func<int, object?> CreateFSharpOptionAccessor<U>(IArrowArray array)
        {
            // 1. Create inner accessor for U
            var innerAccessor = CreateCoreAccessor(array, typeof(U));

            // 2. Wrap result in FSharpOption<U>
            return idx =>
            {
                var val = innerAccessor(idx);
                return val == null ? FSharpOption<U>.None : FSharpOption<U>.Some((U)val);
            };
        }
        private static Func<int, object?> CreateCoreAccessor(IArrowArray array, Type type)
        {
            if (array is DictionaryArray dictArray)
                return CreateDictionaryAccessor(dictArray, type);
            if (array is StructArray sa) return CreateStructAccessor(sa, type);
            
            if (array is ListArray || array is LargeListArray || array is FixedSizeListArray)
                return CreateListAccessor(array, type);

            return CreatePrimitiveAccessor(array, type);
        }

        // =============================================================
        // 1. Primitive Accessor (Optimized)
        // =============================================================
        private static Func<int, object?> CreatePrimitiveAccessor(IArrowArray array, Type type)
        {
            // String
            if (type == typeof(string)) return array.GetStringValue;

            // Boolean
            if (type == typeof(bool)) return idx => ((BooleanArray)array).GetValue(idx);

            // Integers (Unrolled for Performance)
            if (type == typeof(int))    return idx => (int?)array.GetInt64Value(idx);
            if (type == typeof(long))   return idx => array.GetInt64Value(idx);
            if (type == typeof(short))  return idx => (short?)array.GetInt64Value(idx);
            if (type == typeof(byte))   return idx => (byte?)array.GetInt64Value(idx);
            if (type == typeof(sbyte))  return idx => (sbyte?)array.GetInt64Value(idx);
            if (type == typeof(uint))   return idx => (uint?)array.GetInt64Value(idx);
            if (type == typeof(ulong))  return idx => (ulong?)array.GetInt64Value(idx);
            if (type == typeof(ushort)) return idx => (ushort?)array.GetInt64Value(idx);
            // if (type == typeof(Int128)) return idx => array.GetInt128Value(idx);
            // if (type == typeof(UInt128)) return idx => (UInt128?)array.GetInt128Value(idx);

            // Floats
            if (type == typeof(double)) return idx => array.GetDoubleValue(idx);
            if (type == typeof(float))  return idx => (float?)array.GetDoubleValue(idx);
            if (type == typeof(Half))   return idx => (Half?)array.GetDoubleValue(idx);

            // Decimal
            if (type == typeof(decimal))
            {
                if (array is Decimal128Array decArr) return idx => decArr.GetValue(idx); // Fast path
                if (array is DoubleArray dArr) return idx => (decimal?)dArr.GetValue(idx);
                return _ => null;
            }

            // Dates & Times
            if (type == typeof(DateTime))
            {
                if (array is TimestampArray tsArr) return idx => tsArr.GetTimestamp(idx)?.DateTime;
                
                if (array is Date64Array d64Arr) return idx => d64Arr.GetDateTime(idx);
                
                if (array is Date32Array d32Arr) return idx => d32Arr.GetDateTime(idx);

                if (array is Int64Array i64Arr) 
                {
                     return idx => 
                     {
                         long? v = i64Arr.GetValue(idx);
                         return v.HasValue ? DateTime.UnixEpoch.AddTicks(v.Value * 10) : null;
                     };
                }

                return _ => throw new NotSupportedException($"Cannot read DateTime from Arrow Array type: {array.GetType().Name}");;
            }
            
            if (type == typeof(DateOnly)) return idx => array.GetDateOnly(idx);
            
            if (type == typeof(TimeOnly)) return idx => array.GetTimeOnly(idx);
            
            if (type == typeof(TimeSpan)) 
            {
                return idx => array.GetTimeSpan(idx);
            }

            if (type == typeof(DateTimeOffset))
            {
                // Prepare TimeZone Info ONCE, not per row
                TimeZoneInfo? tzi = null;
                if (array is TimestampArray tsArr && tsArr.Data.DataType is TimestampType tsType && !string.IsNullOrEmpty(tsType.Timezone))
                {
                    try { tzi = TimeZoneInfo.FindSystemTimeZoneById(tsType.Timezone); } catch { }
                }
                return idx => array.GetDateTimeOffsetOptimized(idx, tzi);
            }

            // Binary
            if (type == typeof(byte[]))
            {
                 if (array is BinaryArray ba) return idx => ba.GetBytes(idx).ToArray();
                 if (array is LargeBinaryArray lba) return idx => lba.GetBytes(idx).ToArray();
                 if (array is FixedSizeBinaryArray fba) return idx => fba.GetBytes(idx).ToArray();
            }

            return _ => null;
        }
        // =============================================================
        // 2. Struct Accessor
        // =============================================================
        private static Func<int, object?> CreateStructAccessor(StructArray structArray, Type type)
        {
            if (type == typeof(Dictionary<string, object>) || type == typeof(object))
            {
                var fieldAccessors = new Dictionary<string, Func<int, object?>>();
                var fields = ((StructType)structArray.Data.DataType).Fields;
                
                for (int i = 0; i < fields.Count; i++)
                {
                    var fieldName = fields[i].Name;
                    var childArray = structArray.Fields[i];
                    fieldAccessors[fieldName] = CreateAccessor(childArray, typeof(object));
                }

                return idx =>
                {
                    if (structArray.IsNull(idx)) return null;
                    var dict = new Dictionary<string, object?>(fieldAccessors.Count);
                    foreach (var kv in fieldAccessors)
                    {
                        dict[kv.Key] = kv.Value(idx);
                    }
                    return dict;
                };
            }

            // POCO 
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                            .Where(p => p.CanWrite).ToArray();
            
            var structType = (StructType)structArray.Data.DataType;
            var setters = new List<Action<object, int>>();

            foreach (var prop in props)
            {
                // Find Field Index
                int fieldIndex = -1;
                for (int k = 0; k < structType.Fields.Count; k++)
                {
                    if (string.Equals(structType.Fields[k].Name, prop.Name, StringComparison.OrdinalIgnoreCase)) 
                    { 
                        fieldIndex = k; break; 
                    }
                }
                if (fieldIndex == -1) continue;

                var childArray = structArray.Fields[fieldIndex];
                var childGetter = CreateAccessor(childArray, prop.PropertyType);

                setters.Add((obj, rowIdx) => 
                {
                    var val = childGetter(rowIdx);
                    if (val != null) prop.SetValue(obj, val);
                });
            }

            return idx => 
            {
                if (structArray.IsNull(idx)) return null;
                var instance = Activator.CreateInstance(type)!;
                foreach (var setter in setters) setter(instance, idx);
                return instance;
            };
        }

        // =============================================================
        // 3. List Accessor
        // =============================================================
        private static Func<int, object?> CreateListAccessor(IArrowArray array, Type type)
        {
            // Determine Element Type
            Type elementType = typeof(object);
            if (type.IsGenericType) elementType = type.GetGenericArguments()[0];
            else if (type.IsArray) elementType = type.GetElementType()!;
            
            bool isFSharpList = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FSharpList<>);

            // Normalize Array Access
            IArrowArray valuesArray;
            Func<int, long> getOffset;
            Func<int, bool> isNull;

            if (array is ListArray listArr)
            {
                valuesArray = listArr.Values;
                getOffset = i => listArr.ValueOffsets[i];
                isNull = listArr.IsNull;
            }
            else if (array is LargeListArray largeArr)
            {
                valuesArray = largeArr.Values;
                getOffset = i => largeArr.ValueOffsets[i];
                isNull = largeArr.IsNull;
            }
            else
            {
                var fixedArr = (FixedSizeListArray)array;
                valuesArray = fixedArr.Values;
                int width = ((FixedSizeListType)fixedArr.Data.DataType).ListSize;
                getOffset = i => (long)(i + array.Offset) * width; // Correct Offset logic
                isNull = fixedArr.IsNull;
            }

            var childGetter = CreateAccessor(valuesArray, elementType);

            if (isFSharpList)
            {
                var method = typeof(ArrowReader)
                    .GetMethod(nameof(CreateFSharpListAccessor), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(elementType);
                return (Func<int, object?>)method.Invoke(null, [getOffset, isNull, childGetter])!;
            }

            return idx =>
            {
                if (isNull(idx)) return null;

                long start = getOffset(idx);
                long end = getOffset(idx + 1);
                int count = (int)(end - start);

                // Create List<Element>
                var listType = typeof(List<>).MakeGenericType(elementType);
                var list = (IList)Activator.CreateInstance(listType, count)!;

                for (int k = 0; k < count; k++)
                {
                    var val = childGetter((int)(start + k));
                    list.Add(val);
                }

                if (type.IsArray)
                {
                    var arr = System.Array.CreateInstance(elementType, list.Count);
                    list.CopyTo(arr, 0);
                    return arr;
                }
                return list;
            };
        }

        // Specialized F# List Accessor Builder
        private static Func<int, object?> CreateFSharpListAccessor<U>(
            Func<int, long> getOffset, 
            Func<int, bool> isNull, 
            Func<int, object?> childGetter)
        {
            return idx =>
            {
                if (isNull(idx)) return null; // or FSharpList<U>.Empty? Usually null for "Missing List"

                long start = getOffset(idx);
                long end = getOffset(idx + 1);
                int count = (int)(end - start);

                // Build IEnumerable
                var items = new U[count];
                for (int k = 0; k < count; k++)
                {
                    var val = childGetter((int)(start + k));
                    items[k] = val == null ? default! : (U)val;
                }

                // Call direct F# module (Fastest way)
                return ListModule.OfSeq(items);
            };
        }

        // --- Helper: Check whether is scalartype ---
        private static bool IsScalarType(Type t)
        {
            var underlying = Nullable.GetUnderlyingType(t) ?? t;
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(FSharpOption<>)) return true;

            return underlying.IsPrimitive 
                || underlying == typeof(string)
                || underlying == typeof(decimal)
                || underlying == typeof(DateTime)
                || underlying == typeof(DateOnly)
                || underlying == typeof(TimeOnly)
                || underlying == typeof(TimeSpan)
                || underlying == typeof(DateTimeOffset);
        }
        /// <summary>
        /// Create a high-performance accessor for a single Arrow Array.
        /// Used by Series.AsSeq().
        /// </summary>
        public static Func<int, object?> GetSeriesAccessor<T>(IArrowArray array)
            => CreateAccessor(array, typeof(T));
        /// <summary>
        /// Read single Array index i item
        /// </summary>
        public static T? ReadItem<T>(IArrowArray array, int index)
        {
            var accessor = CreateAccessor(array, typeof(T));
            var val = accessor(index);
            return val == null ? default : (T)val;
        }
        // =============================================================
        // Dictionary Accessor (Categorical)
        // =============================================================
        private static Func<int, object?> CreateDictionaryAccessor(DictionaryArray array, Type targetType)
        {
            // Get Indices
            var indices = array.Indices;
            // Get Dictionary
            var dictionary = array.Dictionary;

            // Build Value Accessor
            var dictAccessor = CreateAccessor(dictionary, targetType);

            // Build Indices Accessor
            
            Func<int, int?> indexGetter = indices switch
            {
                Int8Array   i8  => idx => (int?)i8.GetValue(idx),
                UInt8Array  u8  => idx => (int?)u8.GetValue(idx),
                Int16Array  i16 => idx => (int?)i16.GetValue(idx),
                UInt16Array u16 => idx => (int?)u16.GetValue(idx),
                Int32Array  i32 => idx => i32.GetValue(idx),
                UInt32Array u32 => idx => (int?)u32.GetValue(idx),
                Int64Array  i64 => idx => (int?)i64.GetValue(idx),
                _ => throw new NotSupportedException($"Unsupported Dictionary Index Type: {indices.GetType().Name}")
            };

            // Assemble Accessor
            return idx =>
            {
                var key = indexGetter(idx);
                
                if (key == null) return null; 
                
                return dictAccessor(key.Value);
            };
        }
        // =============================================================
        // High Performance Span / Array Access
        // =============================================================

        /// <summary>
        /// Try access scalar via Span (Zero-Copy)。
        /// Notice：Only return true if there is no nulls in array.
        /// </summary>
        public static bool TryGetSpan<T>(IArrowArray array, out ReadOnlySpan<T> span) 
            where T : struct
        {
            span = default;
            
            // 0. Null Check
            if (array.NullCount != 0) return false;

            // 1. Integers
            if (typeof(T) == typeof(int) && array is Int32Array i32) { span = MemoryMarshal.Cast<int, T>(i32.Values); return true; }
            if (typeof(T) == typeof(long) && array is Int64Array i64) { span = MemoryMarshal.Cast<long, T>(i64.Values); return true; }
            if (typeof(T) == typeof(short) && array is Int16Array i16) { span = MemoryMarshal.Cast<short, T>(i16.Values); return true; }
            if (typeof(T) == typeof(sbyte) && array is Int8Array i8) { span = MemoryMarshal.Cast<sbyte, T>(i8.Values); return true; }
            if (typeof(T) == typeof(byte) && array is UInt8Array u8) { span = MemoryMarshal.Cast<byte, T>(u8.Values); return true; }
            if (typeof(T) == typeof(ushort) && array is UInt16Array u16) { span = MemoryMarshal.Cast<ushort, T>(u16.Values); return true; }
            if (typeof(T) == typeof(uint) && array is UInt32Array u32) { span = MemoryMarshal.Cast<uint, T>(u32.Values); return true; }
            if (typeof(T) == typeof(ulong) && array is UInt64Array u64) { span = MemoryMarshal.Cast<ulong, T>(u64.Values); return true; }

            // 2. Floats
            if (typeof(T) == typeof(double) && array is DoubleArray dbl) { span = MemoryMarshal.Cast<double, T>(dbl.Values); return true; }
            if (typeof(T) == typeof(float) && array is FloatArray flt) { span = MemoryMarshal.Cast<float, T>(flt.Values); return true; }
            if (typeof(T) == typeof(Half) && array is HalfFloatArray half) { span = MemoryMarshal.Cast<Half, T>(half.Values); return true; }

            // 3. Date/Time (Internal Int32/Int64)
            if (typeof(T) == typeof(int) && array is Date32Array d32) { span = MemoryMarshal.Cast<int, T>(d32.Values); return true; }
            if (typeof(T) == typeof(long) && array is Date64Array d64) { span = MemoryMarshal.Cast<long, T>(d64.Values); return true; }

            return false;
        }

        /// <summary>
        /// Read Arrow Array by memcpy(non-null primitive types) or accessor.
        /// </summary>
        public static T[] ReadColumn<T>(IArrowArray array)
        {
            // Memcpy
            var fastArray = PrimitiveArrayReader<T>.Read(array);
            if (fastArray != null)
            {
                return fastArray;
            }

            // Accessor
            int len = array.Length;
            var accessor = CreateAccessor(array, typeof(T));
            var result = new T[len];
            
            for (int i = 0; i < len; i++)
            {
                var val = accessor(i);
                result[i] = val == null ? default! : (T)val;
            }
            return result;
        }

        // =============================================================
        // Internal Infrastructure 
        // =============================================================

        private static T[]? ReadStructInternal<T>(IArrowArray array) where T : struct
        {
            if (TryGetSpan<T>(array, out var span))
            {
                return span.ToArray(); 
            }
            return null;
        }

        private static class PrimitiveArrayReader<T>
        {
            public static readonly Func<IArrowArray, T[]?> Read;

            static PrimitiveArrayReader()
            {
                if (typeof(T).IsValueType && Nullable.GetUnderlyingType(typeof(T)) == null)
                {
                    var method = typeof(ArrowReader)
                        .GetMethod(nameof(ReadStructInternal), BindingFlags.NonPublic | BindingFlags.Static)!
                        .MakeGenericMethod(typeof(T));
                    
                    Read = (Func<IArrowArray, T[]?>)Delegate.CreateDelegate(typeof(Func<IArrowArray, T[]?>), method);
                }
                else
                {
                    // for string, Object, Nullable<int> 
                    Read = _ => null;
                }
            }
        }
    }
}