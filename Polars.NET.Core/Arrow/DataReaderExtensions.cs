using System.Collections;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow
{
    public static class DataReaderExtensions
    {
        public static IEnumerable<RecordBatch> ToArrowBatches(this IDataReader reader, int batchSize = 50_000)
        {
            // 1. Schema
            var schema = GetArrowSchema(reader);
            var colCount = reader.FieldCount;

            // 2. 缓冲池 (Buffer Pool)
            var buffers = new IColumnBuffer[colCount];
            for (int i = 0; i < colCount; i++)
            {
                Type netType = reader.GetFieldType(i);
                buffers[i] = ColumnBufferFactory.Create(netType, batchSize);
            }

            int rowCount = 0;
            bool hasYielded = false;

            //Debug
            int totalRowsRead = 0;

            // 3. 泵
            while (reader.Read())
            {
                // [监控点 1] 监控这一行的数据
                // 如果是在第 100 行左右，打印一下关键信息
                totalRowsRead++;
                if (totalRowsRead >= 99 && totalRowsRead <= 102)
                {
                    var val0 = reader.GetValue(0);
                }
                for (int i = 0; i < colCount; i++)
                {
                    buffers[i].Add(reader.GetValue(i));
                }

                rowCount++;

                if (rowCount >= batchSize)
                {
                    yield return Flush(schema, buffers, rowCount);
                    rowCount = 0;
                    hasYielded = true;
                }
            }


            // 4. 扫尾
            if (rowCount > 0)
            {
                yield return Flush(schema, buffers, rowCount);
                hasYielded = true;
            }
            
            // 5. 空流兜底
            if (!hasYielded)
            {
                yield return Flush(schema, buffers, 0);
            }
        }

        private static RecordBatch Flush(Schema schema, IColumnBuffer[] buffers, int length)
        {
            var arrays = new List<IArrowArray>(buffers.Length);
            for (int i = 0; i < buffers.Length; i++)
            {
                var buffer = buffers[i];
                
                // [监控点 3] 构建前检查 Buffer 实际大小
                if (buffer.Count != length)
                {
                    Console.WriteLine($"[CRITICAL PANIC PREDICTION] Column {i} ('{schema.GetFieldByIndex(i).Name}') has {buffer.Count} elements, but we are creating a batch of length {length}!");
                    // 这就是 101 vs 100 的根源！如果这里触发了，我们就破案了。
                }

                var array = buffer.BuildArray();
                
                // [监控点 4] 构建后检查 Array 实际长度
                if (array.Length != length)
                {
                    Console.WriteLine($"[CRITICAL ARROW ERROR] Column {i} BuildArray() returned length {array.Length}, expected {length}. ArrowConverter might be adding extras?");
                }

                arrays.Add(array);
                buffer.Clear();
            }
            return new RecordBatch(schema, arrays, length);
        }

        // Schema 映射
        public static Schema GetArrowSchema(IDataReader reader)
        {
            {
                var fields = new List<Field>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    var type = reader.GetFieldType(i); // 这里可能返回 int[] 或 CustomObj
                    
                    // [邪修入口]
                    // 1. 基础类型映射
                    // 2. 复杂类型 -> 扔给 ArrowConverter 的反射逻辑去猜 Schema
                    IArrowType arrowType = GetArrowType(type);
                    
                    fields.Add(new Field(name, arrowType, true));
                }
                return new Schema(fields, null);
            }
        }
        private static IArrowType GetArrowType(Type t)
        {
            // [注意] 处理 Nullable<T>，比如 int? 应该被识别为 int
            Type checkType = Nullable.GetUnderlyingType(t) ?? t;

            if (checkType == typeof(int)) return Int32Type.Default;
            if (checkType == typeof(long)) return Int64Type.Default;
            if (checkType == typeof(float)) return FloatType.Default;
            if (checkType == typeof(double)) return DoubleType.Default;
            if (checkType == typeof(decimal)) return DoubleType.Default;
            if (checkType == typeof(bool)) return BooleanType.Default;
            if (checkType == typeof(string)) return StringViewType.Default; // 坚持 StringView!
            if (checkType == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, (string)null!);
            if (checkType == typeof(TimeSpan)) return DurationType.FromTimeUnit(TimeUnit.Microsecond);
            if (checkType == typeof(DateOnly)) return Date32Type.Default;
            if (checkType == typeof(TimeOnly)) return Time64Type.Default;
            // 2. [邪修核心]：检测数组/列表 (IEnumerable)
            if (checkType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(checkType))
            {
                var elemType = GetEnumerableElementType(checkType);
                var childArrowType = GetArrowType(elemType);
                return new LargeListType(new Field("item", childArrowType, true));
            }

            // 3. [邪修进阶]：检测 Struct (POCO)
            // 如果不是上述基础类型，也不是数组，那它就是 POCO / Struct
            if (checkType.IsClass || (checkType.IsValueType && !checkType.IsPrimitive && !checkType.IsEnum))
            {
                return ReflectStructSchema(checkType);
            }

            // 真的不认识了，兜底
            return StringViewType.Default;
        }

        // [新增] 递归反射生成 Struct Schema
        private static StructType ReflectStructSchema(Type t)
        {
            // 这里必须和 ArrowConverter.BuildStructArray 的反射逻辑保持一致
            // 比如过滤掉索引器、接口等
            var properties = t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .Where(p => !p.PropertyType.IsInterface && !p.PropertyType.IsAbstract)
                .Where(p => p.PropertyType != typeof(IntPtr) && p.PropertyType != typeof(UIntPtr));

            var fields = new List<Field>();
            foreach (var prop in properties)
            {
                // 递归获取子字段类型
                var childType = GetArrowType(prop.PropertyType);
                
                // 构造 Field
                fields.Add(new Field(prop.Name, childType, true));
            }

            return new StructType(fields);
        }

        // 辅助反射工具
        private static Type GetEnumerableElementType(Type type)
        {
            if (type.IsArray) return type.GetElementType()!;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return type.GetGenericArguments()[0];
            foreach (var i in type.GetInterfaces())
                if (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    return i.GetGenericArguments()[0];
            return typeof(object);
        }
    }

    // =========================================================
    // 胶水层：连接 DataReader(object) 和 ArrowConverter(T)
    // =========================================================

    internal interface IColumnBuffer
    {
        void Add(object value);
        void Clear();
        IArrowArray BuildArray();
        int Count { get; } // [新增]
        
    }

    internal static class ColumnBufferFactory
    {
        public static IColumnBuffer Create(Type netType, int capacity)
        {
            // [关键] 总是创建 Nullable<T>，这能防止 ArrowConverter 误判为 Struct
            Type targetType = netType;
            // 逻辑微调：
            // 1. 如果是值类型 (int)，转 Nullable<int>
            // 2. 如果是引用类型 (string, int[], CustomClass)，保持原样 (它们本身就能存 null)
            if (netType.IsValueType && Nullable.GetUnderlyingType(netType) == null)
            {
                targetType = typeof(Nullable<>).MakeGenericType(netType);
            }
            
            // 动态创建 TypedColumnBuffer<T>
            // 如果 netType 是 int[]，这里就是 TypedColumnBuffer<int[]>
            // 它的 BuildArray 会调用 ArrowConverter.Build<int[]>
            // ArrowConverter 会识别 IEnumerable，进而调用 BuildListArray
            // 完美闭环！
            var bufferType = typeof(TypedColumnBuffer<>).MakeGenericType(targetType);
            return (IColumnBuffer)Activator.CreateInstance(bufferType, capacity)!;
        }
    }

    internal class TypedColumnBuffer<T> : IColumnBuffer
    {
        private readonly List<T> _buffer;
        private static readonly Func<IEnumerable<T>, IArrowArray> _converter;
        

        static TypedColumnBuffer()
        {
            // 反射 ArrowConverter.Build<T> 并缓存委托
            var method = typeof(ArrowConverter)
                .GetMethod("Build", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)
                ?.MakeGenericMethod(typeof(T));
                
            if (method == null) throw new InvalidOperationException($"ArrowConverter.Build<{typeof(T).Name}> not found.");
            
            _converter = (Func<IEnumerable<T>, IArrowArray>)Delegate.CreateDelegate(typeof(Func<IEnumerable<T>, IArrowArray>), method);
        }

        public TypedColumnBuffer(int capacity)
        {
            _buffer = new List<T>(capacity);
        }

        public void Add(object value)
        {
            if (value == null || value == DBNull.Value)
                _buffer.Add(default!);
            else
                _buffer.Add((T)value); // Unbox
        }

        public void Clear() => _buffer.Clear();

        public IArrowArray BuildArray() => _converter(_buffer);
        // Debug
        public int Count => _buffer.Count;
    }
}