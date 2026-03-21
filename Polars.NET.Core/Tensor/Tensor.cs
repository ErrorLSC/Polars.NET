using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Tensor;

/// <summary>
/// A zero-copy 2D representation of underlying contiguous memory.
/// </summary>
public readonly ref struct TensorSpan2D<T> where T : unmanaged
{
    public readonly ReadOnlySpan<T> Data;
    public readonly int Rows;
    public readonly int Cols;

    public TensorSpan2D(ReadOnlySpan<T> data, int rows, int cols)
    {
        Data = data;
        Rows = rows;
        Cols = cols;
    }
    
    public void Deconstruct(out ReadOnlySpan<T> data, out int rows, out int cols)
    {
        data = Data;
        rows = Rows;
        cols = Cols;
    }
}

public static class PolarsTensor
{
   public static ReadOnlySpan<T> AsReadOnlySpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);

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
    public static TensorSpan2D<T> As2DTensorSpan<T>(this SeriesHandle handle) where T : unmanaged
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

        return new TensorSpan2D<T>(finalSpan, rows, cols);
    }
}