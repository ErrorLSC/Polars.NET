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

    // --- Writers (保持不变) ---

    private class Int32Writer : IColumnWriter<int>
    {
        private readonly Int32Array.Builder _builder = new();
        public void Append(int value) => _builder.Append(value);
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

    private class DoubleWriter : IColumnWriter<double>
    {
        private readonly DoubleArray.Builder _builder = new();
        public void Append(double value) => _builder.Append(value);
        public void AppendNull() => _builder.AppendNull();
        public IArrowArray Build() => _builder.Build();
    }

    private class StringWriter : IColumnWriter<string>
    {
        private readonly StringViewArray.Builder _builder = new();
        public void Append(string value) => _builder.Append(value);
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

    // --- Reader Factory (完全复刻 F#) ---
    
    private static Func<int, T> CreateReader<T>(IArrowArray array)
    {
        // 1. Int
        if (typeof(T) == typeof(int))
        {
            return array switch
            {
                Int32Array arr => (Func<int, T>)(object)((Func<int, int>)(i => arr.GetValue(i).GetValueOrDefault())),
                Int64Array arr => (Func<int, T>)(object)((Func<int, int>)(i => (int)arr.GetValue(i).GetValueOrDefault())),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as int")
            };
        }
        
        // 2. Long
        if (typeof(T) == typeof(long))
        {
            return array switch
            {
                Int64Array arr => (Func<int, T>)(object)((Func<int, long>)(i => arr.GetValue(i).GetValueOrDefault())),
                Int32Array arr => (Func<int, T>)(object)((Func<int, long>)(i => arr.GetValue(i).GetValueOrDefault())),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as long")
            };
        }

        // 3. Double
        if (typeof(T) == typeof(double))
        {
             return array switch
            {
                DoubleArray arr => (Func<int, T>)(object)((Func<int, double>)(i => arr.GetValue(i).GetValueOrDefault())),
                FloatArray arr => (Func<int, T>)(object)((Func<int, double>)(i => arr.GetValue(i).GetValueOrDefault())),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as double")
            };
        }

        // 4. String (完全复刻 F# 逻辑：匹配具体类型 -> 直接调用 GetString)
        if (typeof(T) == typeof(string))
        {
            return array switch
            {
                // F#: | :? StringArray as a -> fun i -> unbox (a.GetString(i))
                StringArray arr => (Func<int, T>)(object)((Func<int, string?>)(i => arr.GetString(i))),
                
                // F#: | :? StringViewArray as a -> fun i -> unbox (a.GetString(i))
                StringViewArray arr => (Func<int, T>)(object)((Func<int, string?>)(i => arr.GetString(i))),
                
                // 补一个 LargeString 以防万一 (Polars 经常用这个)
                LargeStringArray arr => (Func<int, T>)(object)((Func<int, string?>)(i => arr.GetString(i))),

                _ => throw new NotSupportedException($"Array {array.GetType().Name} cannot be read as string")
            };
        }

        // 5. Bool
        if (typeof(T) == typeof(bool))
        {
            return array switch
            {
                BooleanArray arr => (Func<int, T>)(object)((Func<int, bool>)(i => arr.GetValue(i).GetValueOrDefault())),
                _ => throw new NotSupportedException($"Cannot read {array.GetType().Name} as bool")
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

        throw new NotSupportedException($"Unsupported UDF output type: {typeof(U).Name}");
    }

    public static Func<IArrowArray, IArrowArray> Wrap<TIn, TOut>(Func<TIn, TOut> userFunc)
    {
        return (IArrowArray inputArray) =>
        {
            // 简单直接，不再加 try-catch 和 Console 输出，还原纯粹的逻辑
            var reader = CreateReader<TIn>(inputArray);
            var writer = CreateWriter<TOut>();

            int length = inputArray.Length;
            for (int i = 0; i < length; i++)
            {
                if (inputArray.IsNull(i))
                {
                    writer.AppendNull();
                }
                else
                {
                    // 假设 F# 的逻辑里这里如果出错会直接抛出，我们也一样
                    TIn input = reader(i);
                    TOut output = userFunc(input);
                    writer.Append(output);
                }
            }
            return writer.Build();
        };
    }
}