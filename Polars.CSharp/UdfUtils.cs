using System;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.CSharp;

internal static class UdfUtils
{
    private interface IColumnWriter<T>
    {
        void Append(T value);
        void AppendNull();
        IArrowArray Build();
    }

    // --- Writers ---

    private class Int32Writer : IColumnWriter<int>
    {
        private readonly Int32Array.Builder _builder = new();
        public void Append(int value) => _builder.Append(value);
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class NullableInt32Writer : IColumnWriter<int?>
    {
        private readonly Int32Array.Builder _builder = new();
        public void Append(int? value)
        {
            if (value.HasValue) _builder.Append(value.Value);
            else _builder.AppendNull();
        }
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class Int64Writer : IColumnWriter<long>
    {
        private readonly Int64Array.Builder _builder = new();
        public void Append(long value) => _builder.Append(value);
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class NullableInt64Writer : IColumnWriter<long?>
    {
        private readonly Int64Array.Builder _builder = new();
        public void Append(long? value)
        {
            if (value.HasValue) _builder.Append(value.Value);
            else _builder.AppendNull();
        }
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class DoubleWriter : IColumnWriter<double>
    {
        private readonly DoubleArray.Builder _builder = new();
        public void Append(double value) => _builder.Append(value);
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class NullableDoubleWriter : IColumnWriter<double?>
    {
        private readonly DoubleArray.Builder _builder = new();
        public void Append(double? value)
        {
            if (value.HasValue) _builder.Append(value.Value);
            else _builder.AppendNull();
        }
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class StringWriter : IColumnWriter<string>
    {
        private readonly StringViewArray.Builder _builder = new();
        public void Append(string value)
        {
            if (value == null) _builder.AppendNull();
            else _builder.Append(value);
        }
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    
    private class BooleanWriter : IColumnWriter<bool>
    {
        private readonly BooleanArray.Builder _builder = new();
        public void Append(bool value) => _builder.Append(value);
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }
    private class NullableBooleanWriter : IColumnWriter<bool?>
    {
        private readonly BooleanArray.Builder _builder = new();
        public void Append(bool? value)
        {
            if (value.HasValue) _builder.Append(value.Value);
            else _builder.AppendNull();
        }
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }

    // --- Reader Factory (完全复刻 F#) ---
    
    private static Func<int, T> CreateReader<T>(IArrowArray array)
    {
        // 1. Int
        if (typeof(T) == typeof(int))
        {
            return array switch
            {
                Int32Array arr => (Func<int, T>)(object)(Func<int, int>)(i => arr.GetValue(i).GetValueOrDefault()),
                Int64Array arr => (Func<int, T>)(object)(Func<int, int>)(i => (int)arr.GetValue(i).GetValueOrDefault()),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as int")
            };
        }
        if (typeof(T) == typeof(int?))
        {
            return array switch
            {
                Int32Array arr => (Func<int, T>)(object)(Func<int, int?>)(i => arr.GetValue(i)),
                Int64Array arr => (Func<int, T>)(object)(Func<int, int?>)(i => (int?)arr.GetValue(i)),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as int?")
            };
        }
        // 2. Long
        if (typeof(T) == typeof(long))
        {
            return array switch
            {
                Int64Array arr => (Func<int, T>)(object)(Func<int, long>)(i => arr.GetValue(i).GetValueOrDefault()),
                Int32Array arr => (Func<int, T>)(object)(Func<int, long>)(i => arr.GetValue(i).GetValueOrDefault()),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as long")
            };
        }
        if (typeof(T) == typeof(long?))
        {
            return array switch
            {
                Int64Array arr => (Func<int, T>)(object)(Func<int, long?>)(i => arr.GetValue(i)),
                Int32Array arr => (Func<int, T>)(object)(Func<int, long?>)(i => arr.GetValue(i)),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as long?")
            };
        }
        // 3. Double
        if (typeof(T) == typeof(double))
        {
             return array switch
            {
                DoubleArray arr => (Func<int, T>)(object)(Func<int, double>)(i => arr.GetValue(i).GetValueOrDefault()),
                FloatArray arr => (Func<int, T>)(object)(Func<int, double>)(i => arr.GetValue(i).GetValueOrDefault()),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as double")
            };
        }
        if (typeof(T) == typeof(double?))
        {
             return array switch
            {
                DoubleArray arr => (Func<int, T>)(object)(Func<int, double?>)(i => arr.GetValue(i)),
                FloatArray arr => (Func<int, T>)(object)(Func<int, double?>)(i => arr.GetValue(i)),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as double?")
            };
        }
        // 4. String
        if (typeof(T) == typeof(string))
        {
            return array switch
            {
                // F#: | :? StringArray as a -> fun i -> unbox (a.GetString(i))
                StringArray arr => (Func<int, T>)(object)(Func<int, string?>)(i => arr.GetString(i)),
                
                // F#: | :? StringViewArray as a -> fun i -> unbox (a.GetString(i))
                StringViewArray arr => (Func<int, T>)(object)(Func<int, string?>)(i => arr.GetString(i)),
                
                LargeStringArray arr => (Func<int, T>)(object)(Func<int, string?>)(i => arr.GetString(i)),

                _ => throw new NotSupportedException($"Array {array.GetType().Name} cannot be read as string")
            };
        }

        // 5. Bool
        if (typeof(T) == typeof(bool))
        {
            return array switch
            {
                BooleanArray arr => (Func<int, T>)(object)(Func<int, bool>)(i => arr.GetValue(i).GetValueOrDefault()),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as bool")
            };
        }
        if (typeof(T) == typeof(bool?))
        {
            return array switch
            {
                BooleanArray arr => (Func<int, T>)(object)(Func<int, bool?>)(i => arr.GetValue(i)),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as bool?")
            };
        }

        throw new NotSupportedException($"Unsupported UDF input type: {typeof(T).Name}");
    }

    private static IColumnWriter<U> CreateWriter<U>()
    {
        if (typeof(U) == typeof(int)) return (IColumnWriter<U>)(object)new Int32Writer();
        if (typeof(U) == typeof(long)) return (IColumnWriter<U>)(object)new Int64Writer();
        if (typeof(U) == typeof(double)) return (IColumnWriter<U>)(object)new DoubleWriter();
        if (typeof(U) == typeof(string)) return (IColumnWriter<U>)(object)new StringWriter();
        if (typeof(U) == typeof(bool)) return (IColumnWriter<U>)(object)new BooleanWriter();

        if (typeof(U) == typeof(int?)) return (IColumnWriter<U>)(object)new NullableInt32Writer();
        if (typeof(U) == typeof(long?)) return (IColumnWriter<U>)(object)new NullableInt64Writer();
        if (typeof(U) == typeof(double?)) return (IColumnWriter<U>)(object)new NullableDoubleWriter();
        if (typeof(U) == typeof(bool?)) return (IColumnWriter<U>)(object)new NullableBooleanWriter();

        throw new NotSupportedException($"Unsupported UDF output type: {typeof(U).Name}");
    }

    public static Func<IArrowArray, IArrowArray> Wrap<TIn, TOut>(Func<TIn, TOut> userFunc)
    {
        return inputArray =>
        {
            var reader = CreateReader<TIn>(inputArray);
            var writer = CreateWriter<TOut>();

            int length = inputArray.Length;
            for (int i = 0; i < length; i++)
            {
                // 如果输入是 Null，我们目前策略是直接写入 Null (Map 语义通常如此)
                // 除非 TIn 是 Nullable 类型 (如 int?)，这时候我们可能希望让用户自己处理 Null
                
                // 为了兼容 Nullable Input，我们需要稍微调整逻辑：
                // 1. 如果 TIn 是值类型(int)，IsNull 为 true 时必须跳过 userFunc，直接写 null。
                // 2. 如果 TIn 是可空类型(int?)，IsNull 为 true 时 reader 会返回 null，我们应该调用 userFunc(null)。
                
                bool inputIsNull = inputArray.IsNull(i);
                bool inputIsNullableType = Nullable.GetUnderlyingType(typeof(TIn)) != null || typeof(TIn) == typeof(string);

                if (inputIsNull && !inputIsNullableType)
                {
                    writer.AppendNull();
                }
                else
                {
                    // 正常执行 (包括 int? 接收到 null 的情况)
                    TIn input = reader(i);
                    TOut output = userFunc(input);
                    writer.Append(output);
                }
            }
            return writer.Build();
        };
    }
}