using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{

    /// <summary>
    /// Unzip nullable array to data array + validity BITMAP (1 bit per row)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (T[] values, byte[]? validity) UnzipNullable<T>(ReadOnlySpan<T?> data, T defaultValue = default) 
        where T : struct
    {
        if (data.IsEmpty) return ([], null);
        Type t = typeof(T);

        // --- 1 Byte Values (stride 2) ---
        if (IsInt8LayoutCompatible) {
            if (t == typeof(byte))  
                return Reinterpret<byte, T>(UnzipInt8SIMD(CastNullableSpan<T, byte>(data), (byte)(object)defaultValue));
            
            if (t == typeof(sbyte)) 
                return Reinterpret<byte, T>(UnzipInt8SIMD(CastNullableSpan<T, byte>(data), (byte)(sbyte)(object)defaultValue));
        }

        // --- 2 Byte Values (stride 4) ---
        if (IsInt16LayoutCompatible) {
            if (t == typeof(short))  
                return Reinterpret<short, T>(UnzipInt16SIMD(CastNullableSpan<T, short>(data), (short)(object)defaultValue));
            
            if (t == typeof(ushort)) 
                return Reinterpret<short, T>(UnzipInt16SIMD(CastNullableSpan<T, short>(data), (short)(ushort)(object)defaultValue));
            
            if (t == typeof(Half))   
                return Reinterpret<short, T>(UnzipInt16SIMD(CastNullableSpan<T, short>(data), BitConverter.HalfToInt16Bits((Half)(object)defaultValue)));
        }

        // --- 4 Byte Values (stride 8) ---
        if (IsInt32LayoutCompatible) {
            if (t == typeof(int))   
                return Reinterpret<int, T>(UnzipInt32SIMD(CastNullableSpan<T, int>(data), (int)(object)defaultValue));
            
            if (t == typeof(uint))  
                return Reinterpret<int, T>(UnzipInt32SIMD(CastNullableSpan<T, int>(data), (int)(uint)(object)defaultValue));
            
            if (t == typeof(float)) 
                return Reinterpret<int, T>(UnzipInt32SIMD(CastNullableSpan<T, int>(data), BitConverter.SingleToInt32Bits((float)(object)defaultValue)));
        }

        // --- 8 Byte Values (stride 16) ---
        if (IsInt64LayoutCompatible) {
            if (t == typeof(long))   
                return Reinterpret<long, T>(UnzipInt64SIMD(CastNullableSpan<T, long>(data), (long)(object)defaultValue));
            
            if (t == typeof(ulong))  
                return Reinterpret<long, T>(UnzipInt64SIMD(CastNullableSpan<T, long>(data), (long)(ulong)(object)defaultValue));
            
            if (t == typeof(double)) 
                return Reinterpret<long, T>(UnzipInt64SIMD(CastNullableSpan<T, long>(data), BitConverter.DoubleToInt64Bits((double)(object)defaultValue)));
        }

        // --- 16 Byte Values (stride 32) ---
        if (IsInt128TypeA || IsInt128TypeB) {
            if (t == typeof(Int128))  
                return Reinterpret<Int128, T>(UnzipInt128SIMD(CastNullableSpan<T, Int128>(data), (Int128)(object)defaultValue));
            
            if (t == typeof(UInt128)) 
                return Reinterpret<Int128, T>(UnzipInt128SIMD(CastNullableSpan<T, Int128>(data), (Int128)(UInt128)(object)defaultValue));
        }
        
        return UnzipGeneric(data, defaultValue);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (T[] values, byte[]? validity) UnzipNullable<T>(T?[] data, T defaultValue = default) 
        where T : struct
        => UnzipNullable(new ReadOnlySpan<T?>(data), defaultValue);


    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static (T[] values, byte[]? validity) UnzipGeneric<T>(ReadOnlySpan<T?> data, T defaultValue) 
        where T : struct
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<T>(len);
        byte[]? validity = null;

        ref T? srcRef = ref MemoryMarshal.GetReference(data);
        ref T valRef = ref MemoryMarshal.GetArrayDataReference(values);

        UnzipScalarLoop(ref srcRef, ref valRef, ref validity, 0, len, defaultValue);

        return (values, validity);
    }
    // =========================================================================
    // Helpers
    // =========================================================================
    
    // Helper to cast result back
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (T[], byte[]?) Reinterpret<S, T>((S[], byte[]?) res)
        => (Unsafe.As<S[], T[]>(ref res.Item1), res.Item2);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SetBitRef(ref byte baseRef, int bitIndex)
    {
        ref byte target = ref Unsafe.Add(ref baseRef, bitIndex >> 3);
        target |= (byte)(1 << (bitIndex & 7));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<U?> CastNullableSpan<T, U>(ReadOnlySpan<T?> span)
        where T : struct
        where U : struct
    {
        // Get Head Reference of original span
        ref T? srcRef = ref MemoryMarshal.GetReference(span);
        
        // Convert its type to new span
        ref U? dstRef = ref Unsafe.As<T?, U?>(ref srcRef);
        
        return MemoryMarshal.CreateReadOnlySpan(ref dstRef, span.Length);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void UnzipScalarLoop<T>(
        ref T? baseSrcRef,      
        ref T baseValRef,       
        ref byte[]? validity,   
        int startIdx,           
        int len,                
        T defaultValue) where T : struct
    {
        // 1. Init validRef
        ref byte validRef = ref Unsafe.NullRef<byte>();
        if (validity != null) 
            validRef = ref MemoryMarshal.GetArrayDataReference(validity);

        for (int i = startIdx; i < len; i++)
        {
            ref T? v = ref Unsafe.Add(ref baseSrcRef, i);

            if (v.HasValue)
            {
                // 1. Write Value
                Unsafe.Add(ref baseValRef, i) = v.GetValueOrDefault();

                // 2. Input Validity
                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) 
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                    ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                    target |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                if (validity == null)
                {
                    int byteLen = (len + 7) >> 3;
                    validity = new byte[byteLen]; 
                    validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                    int bytesToFill = i >> 3;
                    if (bytesToFill > 0) 
                    {
                        Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                    }
                    int remainingBits = i & 7;
                    if (remainingBits > 0)
                    {
                        Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                    }
                }
                
                Unsafe.Add(ref baseValRef, i) = defaultValue;
            }
        }
    }
}