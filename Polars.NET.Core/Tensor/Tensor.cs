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
        return ExtractPrimitiveSpan<T>(arrowArray);
    }
    /// <summary>
    /// Zero-copy extract 2D Tensor memory via Arrow.
    /// </summary>
    public static ReadOnlyTensorSpan<T> AsTensorSpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);
        
        var (valuesArray, rows, cols) = ExtractListMetadata(arrowArray);
        
        ReadOnlySpan<T> finalSpan = ExtractPrimitiveSpan<T>(valuesArray);

        return new ReadOnlyTensorSpan<T>(finalSpan, [rows, cols]);
    }
    /// <summary>
    /// Zero-copy extract 2D Tensor memory via Arrow, returning a Transposed view (M x N -> N x M).
    /// </summary>
    public static ReadOnlyTensorSpan<T> AsTransposedTensorSpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);
        
        var (valuesArray, rows, cols) = ExtractListMetadata(arrowArray);
        ReadOnlySpan<T> finalSpan = ExtractPrimitiveSpan<T>(valuesArray);

        ReadOnlySpan<nint> transposedShape = [cols, rows];
        ReadOnlySpan<nint> transposedStrides = [1, cols];

        return new ReadOnlyTensorSpan<T>(finalSpan, transposedShape, transposedStrides);
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

    private static (IArrowArray Values, int Rows, int Cols) ExtractListMetadata(IArrowArray arrowArray)
    {
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

        return (valuesArray, rows, cols);
    }

    private static ReadOnlySpan<T> ExtractPrimitiveSpan<T>(IArrowArray array) where T : unmanaged
    {
        EnsureTypeMatch<T>(array.Data.DataType);

        if (array.NullCount > 0)
        {
            throw new InvalidOperationException(
                $"Cannot extract memory: array contains {array.NullCount} null values. " +
                "Tensors require contiguous, non-null data. Please use .FillNull() or .DropNulls() first.");
        }

        if (array.Data.Buffers.Length < 2)
        {
            throw new InvalidOperationException("Invalid Arrow memory layout for primitive type.");
        }

        ReadOnlySpan<byte> byteSpan = array.Data.Buffers[1].Span;
        ReadOnlySpan<T> typedSpan = MemoryMarshal.Cast<byte, T>(byteSpan);

        return typedSpan.Slice(array.Data.Offset, array.Length);
    }
}