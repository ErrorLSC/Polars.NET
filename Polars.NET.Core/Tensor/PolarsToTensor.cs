using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Tensor;

public static partial class ArrowTensorInterop
{
    public static ReadOnlySpan<T> AsReadOnlySpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);
        return ExtractFlatPhysicalSpan<T>(arrowArray, out _);
    }

    public static ReadOnlyTensorSpan<T> AsTensorSpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);
        ReadOnlySpan<T> flatSpan = ExtractFlatPhysicalSpan<T>(arrowArray, out int rows);

        if (rows == 0) return new ReadOnlyTensorSpan<T>(flatSpan, [0, 0]);

        int cols = flatSpan.Length / rows;
        if (flatSpan.Length % rows != 0)
            throw new InvalidOperationException("Jagged arrays cannot be implicitly converted to a 2D Tensor. Please provide a specific shape.");

        return new ReadOnlyTensorSpan<T>(flatSpan, [rows, cols]);
    }

    public static ReadOnlyTensorSpan<T> AsTransposedTensorSpan<T>(this SeriesHandle handle) where T : unmanaged
    {
        IArrowArray arrowArray = PolarsWrapper.SeriesToArrow(handle);
        ReadOnlySpan<T> flatSpan = ExtractFlatPhysicalSpan<T>(arrowArray, out int rows);

        if (rows == 0) return new ReadOnlyTensorSpan<T>(flatSpan, [0, 0], [1, 1]);

        int cols = flatSpan.Length / rows;
        return new ReadOnlyTensorSpan<T>(flatSpan, [cols, rows], [1, cols]);
    }

    public static ReadOnlyTensorSpan<T> AsTensorSpan<T>(this SeriesHandle handle, ReadOnlySpan<nint> shape) where T : unmanaged
    {
        ReadOnlySpan<T> flatSpan = handle.AsReadOnlySpan<T>();

        nint requiredElements = 1;
        foreach (nint dim in shape) requiredElements *= dim;

        if (requiredElements != flatSpan.Length)
            throw new ArgumentException($"Shape mismatch! Memory has {flatSpan.Length} elements, shape requires {requiredElements}.");

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

    private static ReadOnlySpan<T> ExtractFlatPhysicalSpan<T>(IArrowArray array, out int topLevelRows) where T : unmanaged
    {
        topLevelRows = array.Length; 

        IArrowArray current = array;
        int logicalOffset = array.Data.Offset;
        int logicalLength = array.Length;


        while (true)
        {
            if (current is FixedSizeListArray fsList)
            {
                int listSize = ((FixedSizeListType)fsList.Data.DataType).ListSize;

                logicalOffset *= listSize;
                logicalLength *= listSize;
                current = fsList.Values;
            }
            else if (current is ListArray list)
            {
                if (logicalLength == 0)
                {
                    logicalOffset = 0;
                    current = list.Values;
                }
                else
                {
                    int start = list.ValueOffsets[logicalOffset];
                    int end = list.ValueOffsets[logicalOffset + logicalLength];
                    logicalOffset = start;
                    logicalLength = end - start;
                    current = list.Values;
                }
            }
            else
            {
                break;
            }
        }

        EnsureTypeMatch<T>(current.Data.DataType);

        if (current.NullCount > 0)
            throw new InvalidOperationException("Cannot extract Tensor memory: contains null values.");

        if (current.Data.Buffers.Length < 2)
            throw new InvalidOperationException("Invalid Arrow memory layout for primitive type.");

        ReadOnlySpan<byte> byteSpan = current.Data.Buffers[1].Span;
        ReadOnlySpan<T> typedSpan = MemoryMarshal.Cast<byte, T>(byteSpan);

        int finalOffset = current.Data.Offset + logicalOffset;
        return typedSpan.Slice(finalOffset, logicalLength);
    }
}