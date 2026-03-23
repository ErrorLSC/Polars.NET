using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Tensor;

public static partial class ArrowTensorInterop
{
    public static SeriesHandle ImportTensor<T>(string name, ReadOnlyTensorSpan<T> tensor) where T : unmanaged
    {
        // =========================================================
        // Materialization
        // =========================================================
        int totalElements = (int)tensor.FlattenedLength;
        T[] flatArray = new T[totalElements]; 
        tensor.FlattenTo(flatArray); 

        // =========================================================
        // Build Level 0 Arrow
        // =========================================================
        IArrowType baseArrowType = ArrowTypeResolver.GetArrowTypeFromNetType(typeof(T));
        int byteLength = totalElements * Unsafe.SizeOf<T>();
        
        ArrowBuffer dataBuffer = new ArrowBuffer.Builder<byte>(byteLength)
                                    .Append(MemoryMarshal.AsBytes<T>(flatArray))
                                    .Build();

        ArrayData currentData = new(
            dataType: baseArrowType,
            length: totalElements,
            nullCount: 0,
            offset: 0,
            buffers: [ArrowBuffer.Empty, dataBuffer] 
        );

        // =========================================================
        // Dynamic N-Dimensional Wrapping
        // =========================================================
        int currentLength = totalElements;

        for (int i = tensor.Rank - 1; i > 0; i--)
        {
            int listSize = (int)tensor.Lengths[i];
            currentLength /= listSize; 

            var listType = new FixedSizeListType(
                new Field("item", currentData.DataType, nullable: false), 
                listSize);

            // Wrap here
            currentData = new ArrayData(
                dataType: listType,
                length: currentLength,
                nullCount: 0,
                offset: 0,
                buffers: [ArrowBuffer.Empty], 
                children: [currentData]       
            );
        }

        IArrowArray finalArray = ArrowArrayFactory.BuildArray(currentData);
        
        return ArrowFfiBridge.ImportSeries(name, finalArray);
    }

    public static IArrowArray BuildRawPrimitiveArray<T>(ReadOnlySpan<T> span) where T : unmanaged
    {
        IArrowType arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(typeof(T));

        int byteLength = span.Length * Unsafe.SizeOf<T>();
        
        ArrowBuffer dataBuffer = new ArrowBuffer.Builder<byte>(byteLength)
                                    .Append(MemoryMarshal.AsBytes(span))
                                    .Build();

        var arrayData = new ArrayData(
            dataType: arrowType,
            length: span.Length,
            nullCount: 0,
            offset: 0,
            buffers: [ArrowBuffer.Empty, dataBuffer] 
        );

        return ArrowArrayFactory.BuildArray(arrayData);
    }
}