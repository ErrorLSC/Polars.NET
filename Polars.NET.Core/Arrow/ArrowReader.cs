// Polars.NET.Core / Arrow / ArrowReader.cs
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.NET.Core.Arrow
{
    public static class ArrowReader
    {
        /// <summary>
        /// Reads an Arrow RecordBatch into a sequence of C# objects.
        /// </summary>
        public static IEnumerable<T> ReadRecordBatch<T>(RecordBatch batch) where T : new()
        {
            int rowCount = batch.Length;

            // 1. 准备反射元数据
            var type = typeof(T);
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                 .Where(p => p.CanWrite)
                                 .ToArray();

            // 2. 预先绑定列读取器
            var columnAccessors = new Func<int, object?>[properties.Length];

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                var col = batch.Column(prop.Name); // 这一步可以用 Dictionary 优化查找速度

                if (col == null)
                {
                    columnAccessors[i] = _ => null;
                    continue;
                }

                // [核心] 生成列读取器
                columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
            }

            // 3. 遍历行，填充对象
            for (int i = 0; i < rowCount; i++)
            {
                var item = new T();
                for (int p = 0; p < properties.Length; p++)
                {
                    var accessor = columnAccessors[p];
                    var val = accessor(i);
                    if (val != null)
                    {
                        properties[p].SetValue(item, val);
                    }
                }
                yield return item;
            }
        }

        // --- CreateAccessor 逻辑 (直接复用你写好的代码) ---
        private static Func<int, object?> CreateAccessor(IArrowArray array, Type targetType)
        {
             // 引用 Polars.NET.Arrow.ArrowExtensions 里的扩展方法
             var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

             // 1. String
             if (underlyingType == typeof(string))
                 return array.GetStringValue; 

             // 2. Int / Long
             if (underlyingType == typeof(int) || underlyingType == typeof(long))
             {
                 return idx => 
                 {
                     long? val = array.GetInt64Value(idx);
                     if (!val.HasValue) return null;
                     if (underlyingType == typeof(int)) return (int)val.Value;
                     return val.Value;
                 };
             }

             // 3. Double / Float
             if (underlyingType == typeof(double) || underlyingType == typeof(float))
             {
                 return idx => 
                 {
                     double? v = array.GetDoubleValue(idx);
                     if (!v.HasValue) return null;
                     if (underlyingType == typeof(float)) return (float)v.Value;
                     return v.Value;
                 };
             }
             
             // 4. DateTime
             if (underlyingType == typeof(DateTime))
             {
                 return idx => 
                 {
                      DateTime? v = array.GetDateTime(idx);
                      if (!v.HasValue) return null;
                      return v.Value;
                 };
             }

            // 5. Decimal
            if (underlyingType == typeof(decimal))
            {
                return idx =>
                {
                    if (array is Decimal128Array decArr)
                    {
                        return decArr.GetValue(idx); // Arrow 自动处理了 Scale，返回 C# decimal?
                    }
                    // 兼容：如果 Polars 传回的是 Double (还没转 Decimal)，尝试强转
                    if (array is DoubleArray dArr)
                    {
                        var v = dArr.GetValue(idx);
                        return v.HasValue ? (decimal)v.Value : null;
                    }
                    return null;
                };
            }

            // 6. Bool
            if (underlyingType == typeof(bool))
            {
                return idx => 
                {
                    if (array is BooleanArray bArr) return bArr.GetValue(idx);
                    return null;
                };
            }

        // 默认回退 (低效但安全)
        return _ => null;
        }
    }
}