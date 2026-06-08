using Apache.Arrow;
using Polars.NET.Core.Arrow; 
using Polars.NET.Core.Data;

namespace Polars.CSharp;

internal static class UdfUtils
{
    public static Func<IArrowArray, IArrowArray> Wrap<TIn, TOut>(Func<TIn, TOut> userFunc)
    {
        var tIn = typeof(TIn);
        
        bool isNullableValueType = tIn.IsValueType && Nullable.GetUnderlyingType(tIn) != null;
        
        bool isPureValueType = tIn.IsValueType && Nullable.GetUnderlyingType(tIn) == null;

        return inputArray =>
        {
            int length = inputArray.Length;
            var rawGetter = ArrowReader.CreateAccessor(inputArray, tIn);
            var buffer = ColumnBufferFactory.Create(typeof(TOut), length);

            if (isPureValueType)
            {
                for (int i = 0; i < length; i++)
                {
                    if (inputArray.IsNull(i))
                    {
                        buffer.Add(null!);
                    }
                    else
                    {
                        buffer.Add(userFunc((TIn)rawGetter(i)!)!);
                    }
                }
            }

            else if (isNullableValueType)
            {
                for (int i = 0; i < length; i++)
                {
                    if (inputArray.IsNull(i))
                    {
                        TIn nullInstance = default!; 
                        buffer.Add(userFunc(nullInstance)!);
                    }
                    else
                    {
                        buffer.Add(userFunc((TIn)rawGetter(i)!)!);
                    }
                }
            }

            else
            {
                for (int i = 0; i < length; i++)
                {
                    TIn input = inputArray.IsNull(i) ? default! : (TIn)rawGetter(i)!;
                    buffer.Add(userFunc(input)!);
                }
            }

            return buffer.BuildArray();
        };
    }
}