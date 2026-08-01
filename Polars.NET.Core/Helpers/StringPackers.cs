using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    /// Packs a span of nullable .NET strings into the Arrow StringView memory layout.
    /// This enables zero‑copy transfer to Rust/Polars with optimal cache behaviour.
    /// </summary>
    /// <param name="data">Input strings (may contain nulls)</param>
    /// <returns>
    /// Tuple of:
    /// - views: Array of fixed‑size (16‑byte) StringView descriptors
    /// - dataBuffer: Contiguous UTF‑8 buffer for strings longer than 12 bytes (null if none)
    /// - validity: Bitmap indicating null entries (null if all strings are non‑null)
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (ArrowStringView[] views, byte[]? dataBuffer, byte[]? validity) PackStringView(ReadOnlySpan<string?> data)
    {
        int len = data.Length;
        var views = GC.AllocateUninitializedArray<ArrowStringView>(len);
        
        // Lazy‑allocated temporary buffer for long strings; starts empty to avoid any heap pressure
        byte[] rentedBuffer = []; 
        int currentDataOffset = 0;
        
        // Validity bitmap: allocated only on first null to keep the fast path allocation‑free
        byte[]? validity = null;
        int validLen = (len + 7) >> 3;

        fixed (ArrowStringView* pViews = views)
        {
            // Zero‑initialize all view structs (inline data and metadata)
            Unsafe.InitBlock(pViews, 0, (uint)(len * sizeof(ArrowStringView)));

            for (int i = 0; i < len; i++)
            {
                string? s = data[i];
                
                // --- Handle null entry: clear the corresponding validity bit ---
                if (s is null)
                {
                    if (validity is null)
                    {
                        validity = new byte[validLen];
                        Array.Fill(validity, (byte)0xFF); // all valid by default
                    }
                    validity[i >> 3] &= (byte)~(1 << (i & 7));
                    continue;
                }

                // --- Attempt short‑string (SSO) path: write directly into the 12‑byte inline buffer ---
                var inlineSpan = new Span<byte>(pViews[i].InlineData, 12);
                var status = Utf8.FromUtf16(s, inlineSpan, out _, out int bytesWritten, replaceInvalidSequences: false);

                if (status == OperationStatus.Done)
                {
                    // String fits in 12 bytes – no external data buffer needed
                    pViews[i].Length = bytesWritten;
                }
                else
                {
                    // --- Long‑string path: encode into the rented overflow buffer ---
                    // The loop ensures we always have enough space; the first attempt may fail if
                    // the current buffer is too small, then we grow and retry.
                    while (true)
                    {
                        int remainingSpace = rentedBuffer.Length - currentDataOffset;
                        
                        if (remainingSpace > 0)
                        {
                            fixed (byte* pRented = rentedBuffer)
                            {
                                status = Utf8.FromUtf16(
                                    s,
                                    new Span<byte>(pRented + currentDataOffset, remainingSpace),
                                    out _, out bytesWritten,
                                    replaceInvalidSequences: false);
                            }
                        }
                        else
                        {
                            status = OperationStatus.DestinationTooSmall;
                        }

                        if (status == OperationStatus.Done)
                        {
                            // Write succeeded – store length, copy first 4 bytes as prefix,
                            // and record the offset into the external buffer.
                            pViews[i].Length = bytesWritten;
                            
                            fixed (byte* pRented = rentedBuffer)
                            {
                                Unsafe.CopyBlock(pViews[i].Prefix, pRented + currentDataOffset, 4);
                            }
                            
                            pViews[i].BufferIndex = 0;      // single buffer, index is always 0
                            pViews[i].Offset = currentDataOffset;
                            currentDataOffset += bytesWritten;
                            break;
                        }
                        else
                        {
                            // --- Buffer too small – grow the rented buffer ---
                            // Use s.Length * 4 as an absolute upper bound (UTF‑8 can use up to 4 bytes per char
                            // for surrogate pairs). This guarantees that after this allocation, the next
                            // write will always succeed, making the while‑loop almost never iterate twice.
                            int safetyBound = s.Length * 4;
                            int newSize = rentedBuffer.Length == 0 
                                ? Math.Max(512, safetyBound)      // first long string: start with a reasonable size
                                : Math.Max(rentedBuffer.Length * 2, currentDataOffset + safetyBound);
                            
                            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
                            
                            // Preserve already written data
                            if (currentDataOffset > 0)
                            {
                                Buffer.BlockCopy(rentedBuffer, 0, newBuffer, 0, currentDataOffset);
                            }
                            
                            // Return the old buffer to the pool if it was rented
                            if (rentedBuffer.Length > 0)
                            {
                                ArrayPool<byte>.Shared.Return(rentedBuffer);
                            }
                            
                            rentedBuffer = newBuffer;
                            // Loop will retry writing with the new larger buffer
                        }
                    }
                }
            }
        }

        // --- Build the final contiguous data buffer from the rented one ---
        byte[]? dataBuffer = null;
        if (currentDataOffset > 0)
        {
            // Allocate the exact‑sized array that will be passed to Rust.
            // This copy is necessary because the rented buffer may be larger than needed.
            dataBuffer = GC.AllocateUninitializedArray<byte>(currentDataOffset);
            Buffer.BlockCopy(rentedBuffer, 0, dataBuffer, 0, currentDataOffset);
        }
        
        // Return the rented buffer to the pool (if any)
        if (rentedBuffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(rentedBuffer);
        }

        return (views, dataBuffer, validity);
    }
}