using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode; 

namespace Polars.NET.Core.Helpers;

[StructLayout(LayoutKind.Explicit, Size = 16)]
public unsafe struct ArrowStringView
{
    [FieldOffset(0)] public int Length;
    [FieldOffset(4)] public fixed byte InlineData[12];
    [FieldOffset(4)] public fixed byte Prefix[4];
    [FieldOffset(8)] public int BufferIndex;
    [FieldOffset(12)] public int Offset;
}

public static unsafe class StringPacker
{
    /// <summary>
    /// Packs a ReadOnlySpan of strings into Polars' Arrow StringView format.
    /// Safely processes the span sequentially without needing to pin the managed reference types.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (ArrowStringView[] views, byte[]? dataBuffer, byte[]? validity) PackStringView(ReadOnlySpan<string?> data)
    {
        int len = data.Length;
        
        var views = GC.AllocateUninitializedArray<ArrowStringView>(len);
        
        long totalDataSize = 0;
        bool hasNull = false;

        fixed (ArrowStringView* pViews = views)
        {
            Unsafe.InitBlock(pViews, 0, (uint)(len * sizeof(ArrowStringView)));

            // Pass 1: Calculate buffer size and check for nulls
            for (int i = 0; i < len; i++)
            {
                // JIT optimally elides the bounds check here
                string? s = data[i];
                if (s != null)
                {
                    int byteCount = Encoding.UTF8.GetByteCount(s);
                    
                    pViews[i].Length = byteCount; 
                    
                    if (byteCount > 12)
                    {
                        totalDataSize += byteCount;
                    }
                }
                else
                {
                    hasNull = true;
                }
            }
        }

        byte[]? dataBuffer = totalDataSize > 0 
            ? GC.AllocateUninitializedArray<byte>((int)totalDataSize) 
            : null;

        byte[]? validity = null;
        if (hasNull)
        {
            int validLen = (len + 7) >> 3;
            validity = new byte[validLen];
            Array.Fill(validity, (byte)0xFF); 
        }

        int currentDataOffset = 0;
        
        // Pin outputs since they are unmanaged structs/bytes
        fixed (ArrowStringView* pViews = views)
        fixed (byte* pDataBuffer = dataBuffer) 
        fixed (byte* pValid = validity)
        {
            // Pass 2: Encode utf8 directly into buffers
            for (int i = 0; i < len; i++)
            {
                string? s = data[i];

                if (s is null)
                {
                    if (pValid != null)
                    {
                        pValid[i >> 3] &= (byte)~(1 << (i & 7));
                    }
                    continue; 
                }

                int byteCount = pViews[i].Length;

                if (byteCount <= 12)
                {
                    if (byteCount > 0)
                    {
                        Utf8.FromUtf16(
                            s, 
                            new Span<byte>(pViews[i].InlineData, 12), 
                            out _, out _, 
                            replaceInvalidSequences: false
                        );
                    }
                }
                else
                {
                    byte* targetDataPtr = pDataBuffer + currentDataOffset;
                    
                    Utf8.FromUtf16(
                        s, 
                        new Span<byte>(targetDataPtr, byteCount), 
                        out _, out _, 
                        replaceInvalidSequences: false
                    );
                    
                    Unsafe.CopyBlock(pViews[i].Prefix, targetDataPtr, 4);
                    
                    pViews[i].BufferIndex = 0; 
                    pViews[i].Offset = currentDataOffset;
                    
                    currentDataOffset += byteCount;
                }
            }
        }

        return (views, dataBuffer, validity);
    }

    /// <summary>
    /// Overload for standard string arrays to maintain API compatibility.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (ArrowStringView[] views, byte[]? dataBuffer, byte[]? validity) PackStringView(string?[] data) => 
        PackStringView(new ReadOnlySpan<string?>(data));
}