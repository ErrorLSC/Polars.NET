using System.Collections.Generic;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.CSharp.Internals
{
    internal static class ArrowBuilderHelper
    {
        // 专门处理 List<long?> -> ListArray (Int64 Values)
        public static ListArray BuildListArray(List<List<long?>?> data)
        {
            // 1. 准备 Builders
            // Value Builder (存放扁平数据)
            var valueBuilder = new Int64Array.Builder();
            
            // Offset Builder (存放切分点)
            var offsetsBuilder = new Int32Array.Builder();
            offsetsBuilder.Append(0); // 必须以 0 开头

            // Validity Builder (存放 null 列表)
            var validityBuilder = new BooleanArray.Builder();

            int currentOffset = 0;
            int nullCount = 0;

            // 2. 遍历数据
            foreach (var subList in data)
            {
                if (subList == null)
                {
                    // Case: null list
                    validityBuilder.Append(false);
                    offsetsBuilder.Append(currentOffset); // Offset 不变
                    nullCount++;
                }
                else
                {
                    // Case: valid list
                    validityBuilder.Append(true);
                    
                    // 填充子元素
                    foreach (var item in subList)
                    {
                        if (item.HasValue)
                            valueBuilder.Append(item.Value);
                        else
                            valueBuilder.AppendNull();
                    }

                    // 更新 Offset
                    currentOffset += subList.Count;
                    offsetsBuilder.Append(currentOffset);
                }
            }

            // 3. Build Components
            var valuesArray = valueBuilder.Build();
            var offsetsArray = offsetsBuilder.Build();
            var validityArray = validityBuilder.Build();

            // 4. Assemble ListArray
            return new ListArray(
                new ListType(new Int64Type()),
                data.Count,
                offsetsArray.ValueBuffer,
                valuesArray,
                validityArray.ValueBuffer,
                nullCount
            );
        }

        // 你可以照葫芦画瓢，增加 BuildStringListArray, BuildDoubleListArray 等
    }
    internal static class StructBuilderHelper
    {
        public static StructArray BuildStructArray<T>(IEnumerable<T> data)
        {
            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            int length = data.Count();

            // 1. 准备字段定义 (Fields) 和 子数组构建器 (Builders)
            var fields = new List<Apache.Arrow.Field>();
            var builders = new List<IArrowArrayBuilder>();

            foreach (var prop in properties)
            {
                var (fieldType, builder) = CreateBuilderForType(prop.PropertyType);
                
                // 处理可空性
                bool isNullable = Nullable.GetUnderlyingType(prop.PropertyType) != null || !prop.PropertyType.IsValueType;
                
                fields.Add(new Apache.Arrow.Field(prop.Name, fieldType, isNullable));
                builders.Add(builder);
            }

            // Struct 自身的 Validity Bitmap
            var structValidityBuilder = new BooleanArray.Builder();
            int nullCount = 0;

            // 2. 遍历数据，填充子数组
            foreach (var item in data)
            {
                if (item == null)
                {
                    structValidityBuilder.Append(false);
                    nullCount++;
                    
                    // 即使 Struct 是 null，子数组也必须 Append 一个占位符 (通常是 null 或 default)
                    // 否则子数组长度会对不上
                    foreach (var builder in builders)
                    {
                        // 这是一个简化处理，更严谨的做法是 AppendNull
                        // 但 Arrow Builder 接口比较分散，这里简单调用 AppendNull
                        // 注意：你需要扩展 builder 接口或者根据类型 cast
                        AppendNullToBuilder(builder); 
                    }
                }
                else
                {
                    structValidityBuilder.Append(true);
                    
                    // 填充每个属性的值
                    for (int i = 0; i < properties.Length; i++)
                    {
                        var val = properties[i].GetValue(item);
                        AppendValueToBuilder(builders[i], val);
                    }
                }
            }

            // 3. Build 所有子数组
            var childrenArrays = builders.Select(b => BuildArray(b)).ToList();
            var structType = new StructType(fields);
            var structValidity = structValidityBuilder.Build();

            // 4. 组装 StructArray
            return new StructArray(
                structType,
                length,
                childrenArrays,
                structValidity.ValueBuffer,
                nullCount
            );
        }

        // --- 辅助方法：根据类型选择 Builder ---
        private static (IArrowType, IArrowArrayBuilder) CreateBuilderForType(Type type)
        {
            // 处理 Nullable<T>
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            if (underlyingType == typeof(int)) 
                return (new Int32Type(), new Int32Array.Builder());
            
            if (underlyingType == typeof(long)) 
                return (new Int64Type(), new Int64Array.Builder());
            
            if (underlyingType == typeof(double)) 
                return (new DoubleType(), new DoubleArray.Builder());
            
            if (underlyingType == typeof(string)) 
                return (new StringType(), new StringArray.Builder());
            
            if (underlyingType == typeof(bool)) 
                return (new BooleanType(), new BooleanArray.Builder());

            // ⚡ 这里可以递归！如果属性又是 List<T> 或 class，就递归调用
            // if (IsList(type)) ...
            
            throw new NotSupportedException($"Type {type.Name} is not yet supported in Struct auto-mapping.");
        }

        // --- 辅助方法：往 Builder 里塞值 ---
        // 这一块用 dynamic 或者 switch 会比较长，为了演示逻辑简化写
        private static void AppendValueToBuilder(IArrowArrayBuilder builder, object? value)
        {
            if (value == null)
            {
                AppendNullToBuilder(builder);
                return;
            }

            // 使用模式匹配分发
            switch (builder)
            {
                case Int32Array.Builder b: b.Append((int)value); break;
                case Int64Array.Builder b: b.Append(Convert.ToInt64(value)); break; // 宽容转换
                case DoubleArray.Builder b: b.Append((double)value); break;
                case StringArray.Builder b: b.Append((string)value); break;
                case BooleanArray.Builder b: b.Append((bool)value); break;
                default: throw new NotImplementedException();
            }
        }

        private static void AppendNullToBuilder(IArrowArrayBuilder builder)
        {
             switch (builder)
            {
                case Int32Array.Builder b: b.AppendNull(); break;
                case Int64Array.Builder b: b.AppendNull(); break;
                case DoubleArray.Builder b: b.AppendNull(); break;
                case StringArray.Builder b: b.AppendNull(); break;
                case BooleanArray.Builder b: b.AppendNull(); break;
                default: throw new NotImplementedException();
            }
        }
        private static IArrowArray BuildArray(IArrowArrayBuilder builder)
        {
            return builder switch
            {
                Int32Array.Builder b => b.Build(),
                Int64Array.Builder b => b.Build(),
                DoubleArray.Builder b => b.Build(),
                StringArray.Builder b => b.Build(),
                BooleanArray.Builder b => b.Build(),
                
                // 如果是 Struct 递归的情况，这里也需要支持
                // StructArray.Builder b => b.Build(),
                // ListArray.Builder b => b.Build(),

                _ => throw new NotImplementedException($"Builder type {builder.GetType().Name} build not supported.")
            };
        }
    }
}