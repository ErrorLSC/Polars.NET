using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.FSharp.Core;

namespace Polars.NET.Core.Helpers;

public static unsafe class DecimalPacker
{
    // C# decimal mem layout(Sequential): flags, hi, lo, mid (4 int)
    internal static readonly Int128[] PowersOf10Int128;
    static DecimalPacker() // Static Constructor
    {
        PowersOf10Int128 = new Int128[30]; // decimal max scale is 28
        PowersOf10Int128[0] = 1;
        for (int i = 1; i < PowersOf10Int128.Length; i++)
        {
            PowersOf10Int128[i] = PowersOf10Int128[i - 1] * 10;
        }
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (Int128[] values, int scale) Pack(ReadOnlySpan<decimal> data)
    {
        int len = data.Length;
        if (len == 0) return (Array.Empty<Int128>(), 0);

        byte maxScale = 0;

        // Pass 1: Scan Max Scale
        fixed (decimal* pSrc = data)
        {
            // decimal is 16 bytes (4 int)
            int* pInt = (int*)pSrc;
            
            // Unroll 
            for (int i = 0; i < len; i++)
            {
                int flags = pInt[i * 4]; 
                byte s = (byte)((flags >> 16) & 0xFF);
                if (s > maxScale) maxScale = s;
            }
        }

        var values = GC.AllocateUninitializedArray<Int128>(len);

        // Pass 2: Convert
        fixed (decimal* pSrc = data)
        fixed (Int128* pDst = values)
        {
            // pSrc -> decimal[] Array
            // treat it as int array
            int* pRawDec = (int*)pSrc;
            
            for (int i = 0; i < len; i++)
            {
                // Calc decimal int* start position
                int baseIdx = i * 4;
                
                int flags = pRawDec[baseIdx];     // [0] Flags
                int hi    = pRawDec[baseIdx + 1]; // [1] Hi
                int lo    = pRawDec[baseIdx + 2]; // [2] Lo
                int mid   = pRawDec[baseIdx + 3]; // [3] Mid

                // Assemble 96-bit Mantissa -> Int128
                Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                // Handle +- (Flags & highest bit)
                if ((flags & 0x80000000) != 0)
                {
                    mantissa = -mantissa;
                }

                // Rescale
                int currentScale = (flags >> 16) & 0xFF;
                int diff = maxScale - currentScale;
                
                if (diff > 0) 
                {
                    try 
                    {
                        checked 
                        {
                            mantissa *= PowersOf10Int128[diff];
                        }
                    }
                    catch (OverflowException)
                    {
                         throw new OverflowException(
                            $"Decimal overflow at index {i}. " +
                            $"Item cannot be rescaled to Scale {maxScale} without exceeding 38-digit limit.");
                    }
                }

                pDst[i] = mantissa;
            }
        }

        return (values, maxScale);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (Int128[] values, byte[]? validity, int scale) Pack(ReadOnlySpan<decimal?> data)
    {
        int len = data.Length;
        if (len == 0) return ([], null, 0);

        byte maxScale = 0;
        ref decimal? srcRef = ref MemoryMarshal.GetReference(data);

        // Pass 1: Scan max Scale
        for (int i = 0; i < len; i++)
        {
            ref decimal? item = ref Unsafe.Add(ref srcRef, i);
            if (item.HasValue)
            {
                byte s = item.GetValueOrDefault().Scale;
                if (s > maxScale) maxScale = s;
            }
        }
        
        // Pass 2: Convert
        var values = GC.AllocateUninitializedArray<Int128>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        fixed (Int128* pDst = values)
        {
            ref Int128 dstRef = ref MemoryMarshal.GetArrayDataReference(values);

            for (int i = 0; i < len; i++)
            {
                ref decimal? item = ref Unsafe.Add(ref srcRef, i);
                
                if (item.HasValue)
                {
                    decimal d = item.GetValueOrDefault();
                    
                    int* pDec = (int*)Unsafe.AsPointer(ref d);
                    
                    int flags = pDec[0];
                    int hi    = pDec[1];
                    int lo    = pDec[2];
                    int mid   = pDec[3];

                    Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                    if ((flags & 0x80000000) != 0)
                    {
                        mantissa = -mantissa;
                    }
                    
                    int scale = (flags >> 16) & 0xFF;
                    int diff = maxScale - scale;
                    if (diff > 0) 
                    {
                        try 
                        {
                            checked 
                            {
                                mantissa *= PowersOf10Int128[diff];
                            }
                        }
                        catch (OverflowException)
                        {
                            throw new OverflowException(
                                $"Decimal overflow at index {i}. " +
                                $"Item cannot be rescaled to Scale {maxScale} without exceeding 38-digit limit.");
                        }
                    }
                    
                    Unsafe.Add(ref dstRef, i) = mantissa;
                    
                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        validity = new byte[(len + 7) >> 3];
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        int bytesToFill = i >> 3;
                        if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                        int remainingBits = i & 7;
                        if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                    }
                    Unsafe.Add(ref dstRef, i) = Int128.Zero;
                }
            }
        }

        return (values, validity, maxScale);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (Int128[] values, int scale) Pack(decimal* pSrc, int len)
    {
        if (len == 0) return (Array.Empty<Int128>(), 0);

        byte maxScale = 0;
        int* pInt = (int*)pSrc; // Treat decimal as int[4]

        // Pass 1: Scan Scale
        for (int i = 0; i < len; i++)
        {
            int flags = pInt[i * 4];
            byte s = (byte)((flags >> 16) & 0xFF);
            if (s > maxScale) maxScale = s;
        }

        var values = GC.AllocateUninitializedArray<Int128>(len);

        fixed (Int128* pDst = values)
        {
            int* pRawDec = (int*)pSrc;
            
            for (int i = 0; i < len; i++)
            {
                int baseIdx = i * 4;
                int flags = pRawDec[baseIdx];
                int hi    = pRawDec[baseIdx + 1];
                int lo    = pRawDec[baseIdx + 2];
                int mid   = pRawDec[baseIdx + 3];

                Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                if ((flags & 0x80000000) != 0) mantissa = -mantissa;

                int currentScale = (flags >> 16) & 0xFF;
                int diff = maxScale - currentScale;
                
                if (diff > 0) 
                {
                    try 
                    {
                        checked 
                        {
                            mantissa *= PowersOf10Int128[diff];
                        }
                    }
                    catch (OverflowException)
                    {

                         throw new OverflowException(
                            $"Decimal overflow at index {i}. " +
                            $"Item cannot be rescaled to Scale {maxScale} without exceeding 38-digit limit.");
                    }
                }

                pDst[i] = mantissa;
            }
        }

        return (values, maxScale);
    }
    
    // ========================================================================
    // F# Support (Direct Packing without intermediate array)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<decimal> directly to Int128[] + Validity
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (Int128[] values, byte[]? validity, int scale) Pack(ReadOnlySpan<FSharpOption<decimal>> data)
    {
        int len = data.Length;
        if (len == 0) return ([], null, 0);
        
        byte maxScale = 0;
        ref FSharpOption<decimal> srcRef = ref MemoryMarshal.GetReference(data);
        
        for (int i = 0; i < len; i++)
        {
            FSharpOption<decimal> item = Unsafe.Add(ref srcRef, i);
            if (item != null) 
            {
                byte s = item.Value.Scale; 
                if (s > maxScale) maxScale = s;
            }
        }
        
        var values = GC.AllocateUninitializedArray<Int128>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        fixed (Int128* pDst = values)
        {
            ref Int128 dstRef = ref MemoryMarshal.GetArrayDataReference(values);

            for (int i = 0; i < len; i++)
            {
                FSharpOption<decimal> item = Unsafe.Add(ref srcRef, i);
                
                if (item != null)
                {
                    decimal d = item.Value;
                    
                    int* pDec = (int*)Unsafe.AsPointer(ref d);
                    int flags = pDec[0];
                    int hi    = pDec[1];
                    int lo    = pDec[2];
                    int mid   = pDec[3];

                    Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                    if ((flags & 0x80000000) != 0) mantissa = -mantissa;
                    
                    int currentScale = (flags >> 16) & 0xFF;
                    int diff = maxScale - currentScale;
                    if (diff > 0) 
                    {
                        try { checked { mantissa *= PowersOf10Int128[diff]; } }
                        catch (OverflowException) { throw new OverflowException($"Decimal overflow at index {i}."); }
                    }
                    
                    Unsafe.Add(ref dstRef, i) = mantissa;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        validity = new byte[(len + 7) >> 3];
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        int bytesToFill = i >> 3;
                        if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                        int remainingBits = i & 7;
                        if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                    }
                    Unsafe.Add(ref dstRef, i) = Int128.Zero;
                }
            }
        }
        return (values, validity, maxScale);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (Int128[] values, byte[]? validity, int scale) Pack(ReadOnlySpan<FSharpValueOption<decimal>> data)
    {
        int len = data.Length;
        if (len == 0) return (Array.Empty<Int128>(), null, 0);

        byte maxScale = 0;
        ref FSharpValueOption<decimal> srcRef = ref MemoryMarshal.GetReference(data);
        
        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<decimal> item = ref Unsafe.Add(ref srcRef, i);
            if (item.Tag == FSharpValueOption<decimal>.Tags.ValueSome)
            {
                byte s = item.Value.Scale;
                if (s > maxScale) maxScale = s;
            }
        }
        
        var values = GC.AllocateUninitializedArray<Int128>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        fixed (Int128* pDst = values)
        {
            ref Int128 dstRef = ref MemoryMarshal.GetArrayDataReference(values);

            for (int i = 0; i < len; i++)
            {
                ref FSharpValueOption<decimal> item = ref Unsafe.Add(ref srcRef, i);
                
                if (item.Tag == FSharpValueOption<decimal>.Tags.ValueSome)
                {
                    decimal d = item.Value;
                    
                    int* pDec = (int*)Unsafe.AsPointer(ref d);
                    int flags = pDec[0];
                    int hi    = pDec[1];
                    int lo    = pDec[2];
                    int mid   = pDec[3];

                    Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                    if ((flags & 0x80000000) != 0) mantissa = -mantissa;
                    
                    int currentScale = (flags >> 16) & 0xFF;
                    int diff = maxScale - currentScale;
                    if (diff > 0) 
                    {
                        try { checked { mantissa *= PowersOf10Int128[diff]; } }
                        catch (OverflowException) { throw new OverflowException($"Decimal overflow at index {i}."); }
                    }
                    
                    Unsafe.Add(ref dstRef, i) = mantissa;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        validity = new byte[(len + 7) >> 3];
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        int bytesToFill = i >> 3;
                        if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                        int remainingBits = i & 7;
                        if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                    }
                    Unsafe.Add(ref dstRef, i) = Int128.Zero;
                }
            }
        }
        return (values, validity, maxScale);
    }
    // ========================================================================
    // Array Overloads for backward API compatibility (Inlined)
    // ========================================================================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Int128[] values, int scale) Pack(decimal[] data) 
        => Pack(new ReadOnlySpan<decimal>(data));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Int128[] values, byte[]? validity, int scale) Pack(decimal?[] data) 
        => Pack(new ReadOnlySpan<decimal?>(data));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Int128[] values, byte[]? validity, int scale) Pack(FSharpOption<decimal>[] data) 
        => Pack(new ReadOnlySpan<FSharpOption<decimal>>(data));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Int128[] values, byte[]? validity, int scale) Pack(FSharpValueOption<decimal>[] data) 
        => Pack(new ReadOnlySpan<FSharpValueOption<decimal>>(data));
}