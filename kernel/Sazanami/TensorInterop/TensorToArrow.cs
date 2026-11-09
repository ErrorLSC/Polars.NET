using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Sazanami.TensorInterop;

public static partial class ArrowTensorInterop
{
    public static IArrowArray ToArrowArray<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(ReadOnlyTensorSpan<T> tensor) where T : unmanaged
    {
        int totalElements = (int)tensor.FlattenedLength;
        int byteLength = totalElements * Unsafe.SizeOf<T>();

        T[] pooledArray = ArrayPool<T>.Shared.Rent(totalElements);

        ArrowBuffer dataBuffer;

        try
        {
            Span<T> flatSpan = pooledArray.AsSpan(0, totalElements);

            // memcpy 1
            tensor.FlattenTo(flatSpan);

            // memcpy 2
            dataBuffer = new ArrowBuffer.Builder<byte>(byteLength)
                            .Append(MemoryMarshal.AsBytes(flatSpan))
                            .Build();
        }
        finally
        {
            ArrayPool<T>.Shared.Return(pooledArray);
        }

        IArrowType baseArrowType = ArrowTypeResolver.GetArrowTypeFromNetType(typeof(T));

        ArrayData currentData = new(
            dataType: baseArrowType,
            length: totalElements,
            nullCount: 0,
            offset: 0,
            buffers: [ArrowBuffer.Empty, dataBuffer]
        );

        int currentLength = totalElements;

        for (int i = tensor.Rank - 1; i > 0; i--)
        {
            int listSize = (int)tensor.Lengths[i];
            currentLength /= listSize;

            var listType = new FixedSizeListType(
                new Field("item", currentData.DataType, nullable: false),
                listSize);

            currentData = new ArrayData(
                dataType: listType,
                length: currentLength,
                nullCount: 0,
                offset: 0,
                buffers: [ArrowBuffer.Empty],
                children: [currentData]
            );
        }

        return ArrowArrayFactory.BuildArray(currentData);
    }

    public static IArrowArray BuildRawPrimitiveArray<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(ReadOnlySpan<T> span) where T : unmanaged
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