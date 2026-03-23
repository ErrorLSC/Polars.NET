using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Tensor;

public static class PolarsTensor
{
   public static ReadOnlySpan<T> AsReadOnlySpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);

        EnsureTypeMatch<T>(arrowArray.Data.DataType);

        if (arrowArray.NullCount > 0)
        {
            throw new InvalidOperationException(
                $"Cannot create ReadOnlySpan from Series: contains {arrowArray.NullCount} null values. " +
                "Tensors require contiguous, non-null data. Please use .FillNull() or .DropNulls() first.");
        }

        if (arrowArray.Data.Buffers.Length < 2)
        {
            throw new InvalidOperationException("Invalid Arrow memory layout for primitive type.");
        }

        ArrowBuffer dataBuffer = arrowArray.Data.Buffers[1];
        ReadOnlySpan<byte> byteSpan = dataBuffer.Span;

        ReadOnlySpan<T> typedSpan = MemoryMarshal.Cast<byte, T>(byteSpan);

        return typedSpan.Slice(arrowArray.Data.Offset, arrowArray.Length);
    }
    /// <summary>
    /// Zero-copy extract 2D Tensor memory via Arrow.
    /// </summary>
    public static ReadOnlyTensorSpan<T> AsTensorSpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);
        
        int rows = arrowArray.Length;
        int cols = 0;
        IArrowArray valuesArray;

        if (arrowArray is FixedSizeListArray fsList)
        {
            valuesArray = fsList.Values;
            cols = ((FixedSizeListType)fsList.Data.DataType).ListSize; 
        }
        else if (arrowArray is ListArray listArray)
        {
            if (rows > 0)
            {
                cols = listArray.ValueOffsets[1] - listArray.ValueOffsets[0]; 
                
                if (listArray.Values.Length != rows * cols)
                {
                    throw new InvalidOperationException(
                        "Cannot convert variable-length List to Tensor. " +
                        "All sub-lists must have the exact same length (dimension) to form a 2D matrix.");
                }
            }
            valuesArray = listArray.Values;
        }
        else
        {
            throw new InvalidOperationException(
                $"Cannot extract 2D Tensor from Arrow type '{arrowArray.Data.DataType.Name}'. " +
                "Underlying Series memory must be a List or FixedSizeList layout.");
        }

        EnsureTypeMatch<T>(valuesArray.Data.DataType);

        if (valuesArray.NullCount > 0)
        {
            throw new InvalidOperationException("The inner values of the list contain nulls. Tensors require dense, non-null data.");
        }

        if (valuesArray.Data.Buffers.Length < 2)
        {
            throw new InvalidOperationException("Invalid Arrow memory layout for primitive inner values.");
        }

        ArrowBuffer dataBuffer = valuesArray.Data.Buffers[1];
        ReadOnlySpan<byte> byteSpan = dataBuffer.Span;
        ReadOnlySpan<T> typedSpan = MemoryMarshal.Cast<byte, T>(byteSpan);

        ReadOnlySpan<T> finalSpan = typedSpan.Slice(valuesArray.Data.Offset, valuesArray.Length);


        ReadOnlySpan<nint> dimensions = [rows, cols];
        return new ReadOnlyTensorSpan<T>(finalSpan, dimensions);
    }
    /// <summary>
    /// Zero-copy extract Arrow memory and reshape it into a standard .NET TensorSpan.
    /// </summary>
    /// <param name="shape">The desired dimensions. Total elements must match the underlying Span.</param>
    public static ReadOnlyTensorSpan<T> AsTensorSpan<T>(this SeriesHandle handle, ReadOnlySpan<nint> shape) 
        where T : unmanaged
    {
        ReadOnlySpan<T> flatSpan = handle.AsReadOnlySpan<T>();

        nint requiredElements = 1;
        foreach (nint dim in shape)
        {
            requiredElements *= dim;
        }

        if (requiredElements != flatSpan.Length)
        {
            throw new ArgumentException(
                $"Shape mismatch! The underlying memory has {flatSpan.Length} elements, " +
                $"but the requested shape {string.Join("x", shape.ToArray())} requires {requiredElements} elements.");
        }

        return new ReadOnlyTensorSpan<T>(flatSpan, shape);
    }
    private static void EnsureTypeMatch<T>(IArrowType arrowType) where T : unmanaged
    {
        Type expectedNetType = ArrowTypeResolver.GetNetTypeFromArrowType(arrowType);

        if (typeof(T) != expectedNetType)
        {
            throw new InvalidOperationException(
                $"Type mismatch! The underlying Arrow type is '{arrowType.Name}', which maps to .NET type '{expectedNetType.Name}'. " +
                $"Cannot safely cast it to requested Tensor type '{typeof(T).Name}'.");
        }
    }
}