using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.Wrappers;

/// <summary>The cast rules of a Parquet scan, one bit per rule (the engine's <c>CastColumnsPolicy</c>).</summary>
[Flags]
public enum PlScanCastFlags : uint
{
    None = 0,
    IntegerUpcast = 1 << 0,
    IntegerToFloat = 1 << 1,
    FloatUpcast = 1 << 2,
    FloatDowncast = 1 << 3,
    DatetimeNanosecondDowncast = 1 << 4,
    DatetimeMicrosecondDowncast = 1 << 5,
    DatetimeMillisecondUpcast = 1 << 6,
    DatetimeMicrosecondUpcast = 1 << 7,
    DatetimeConvertTimeZone = 1 << 8,
    CategoricalToString = 1 << 9,
    InsertMissingStructFields = 1 << 10,
    IgnoreExtraStructFields = 1 << 11,
}

/// <summary>
/// Key material of Parquet modular encryption: the footer key, the keys of columns encrypted with their own key
/// (by dot-separated Parquet path), and the AAD prefix (empty or null: none).
/// </summary>
public readonly record struct ParquetKeys(
    ReadOnlyMemory<byte> FooterKey,
    IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? ColumnKeys,
    ReadOnlyMemory<byte>? AadPrefix);

public readonly partial struct PolarsWrapper
{
    /// <summary>The scan extras, or null when every option has its default (the engine then takes a null handle).</summary>
    public static ParquetScanExtrasHandle? ParquetScanExtras(
        bool ignoreExtraColumns,
        PlScanCastFlags castFlags,
        string[]? hiddenFilePrefixes,
        ParquetKeys? decryption,
        bool verifyFooterSignature)
    {
        if (!ignoreExtraColumns && castFlags == PlScanCastFlags.None && hiddenFilePrefixes is not { Length: > 0 } && decryption is null)
            return null;
        if (decryption is { } keys) CheckEngineKeys(keys, "decryption");

        unsafe
        {
            using var pins = new KeyPins(decryption);
            var h = NativeBindings.pl_parquet_scan_extras_new(
                ignoreExtraColumns,
                (uint)castFlags,
                hiddenFilePrefixes,
                (nuint)(hiddenFilePrefixes?.Length ?? 0),
                pins.FooterKey, pins.FooterKeyLength,
                pins.ColumnNames, pins.ColumnKeys, pins.ColumnKeyLengths, pins.ColumnCount,
                pins.AadPrefix, pins.AadPrefixLength,
                verifyFooterSignature);
            return ErrorHelper.Check(h);
        }
    }

    /// <summary>The write extras, or null when every option has its default.</summary>
    public static ParquetWriteExtrasHandle? ParquetWriteExtras(
        IReadOnlyDictionary<string, string>? metadata,
        Schema? arrowSchema,
        ParquetKeys? encryption,
        bool plaintextFooter,
        bool storeAadPrefix)
    {
        if (metadata is not { Count: > 0 } && arrowSchema is null && encryption is null)
            return null;
        if (encryption is { } keys) CheckEngineKeys(keys, "encryption");

        string[]? metadataKeys = metadata is { Count: > 0 } ? [.. metadata.Keys] : null;
        string[]? metadataValues = metadataKeys is null ? null : [.. metadataKeys.Select(k => metadata![k])];

        unsafe
        {
            CArrowSchema* cSchema = null;
            try
            {
                if (arrowSchema is not null)
                {
                    cSchema = CArrowSchema.Create();
                    CArrowSchemaExporter.ExportSchema(arrowSchema, cSchema);
                }
                using var pins = new KeyPins(encryption);
                var h = NativeBindings.pl_parquet_write_extras_new(
                    metadataKeys,
                    metadataValues,
                    (nuint)(metadataKeys?.Length ?? 0),
                    cSchema,
                    pins.FooterKey, pins.FooterKeyLength,
                    pins.ColumnNames, pins.ColumnKeys, pins.ColumnKeyLengths, pins.ColumnCount,
                    plaintextFooter,
                    pins.AadPrefix, pins.AadPrefixLength,
                    storeAadPrefix);
                return ErrorHelper.Check(h);
            }
            finally
            {
                if (cSchema is not null) CArrowSchema.Free(cSchema);
            }
        }
    }

    /// <summary>The engine (Polars) takes AES-128 and AES-256 keys only; a refusal names the key and the lengths.</summary>
    private static void CheckEngineKeys(ParquetKeys keys, string use)
    {
        Check(keys.FooterKey.Length, "the footer key");
        if (keys.ColumnKeys is null) return;
        foreach (var (column, key) in keys.ColumnKeys)
        {
            if (string.IsNullOrEmpty(column))
                throw new ArgumentException($"Parquet {use}: a column key needs the column's dot-separated Parquet path, got an empty name.");
            Check(key.Length, $"the key of column '{column}'");
        }

        void Check(int length, string which)
        {
            if (length is not (16 or 32))
                throw new ArgumentException(
                    $"Parquet {use}: {which} is {length} bytes; the engine takes 16-byte (AES-128) or 32-byte (AES-256) keys. " +
                    "(The managed Seseragi Parquet reader and writer also take 24-byte AES-192 keys.)");
        }
    }

    /// <summary>The key material pinned for the length of one native call (the engine copies it).</summary>
    private unsafe ref struct KeyPins
    {
        private MemoryHandle _footer;
        private MemoryHandle _aad;
        private readonly MemoryHandle[]? _columns;
        private readonly nint* _columnPointers;
        private readonly nuint* _columnLengths;

        public byte* FooterKey;
        public nuint FooterKeyLength;
        public byte* AadPrefix;
        public nuint AadPrefixLength;
        public string[]? ColumnNames;
        public nuint ColumnCount;
        public readonly byte** ColumnKeys => (byte**)_columnPointers;
        public readonly nuint* ColumnKeyLengths => _columnLengths;

        public KeyPins(ParquetKeys? keys)
        {
            if (keys is not { } k) return;
            // A non-null pointer means "a key was given", also for an empty one (which the engine refuses).
            _footer = k.FooterKey.Pin();
            FooterKey = _footer.Pointer is null ? (byte*)NativeMemory.Alloc(1) : (byte*)_footer.Pointer;
            FooterKeyLength = (nuint)k.FooterKey.Length;
            // An empty prefix adds nothing to the AAD: it means none (a null byte[] converts to an empty memory).
            if (k.AadPrefix is { Length: > 0 } aad)
            {
                _aad = aad.Pin();
                AadPrefix = (byte*)_aad.Pointer;
                AadPrefixLength = (nuint)aad.Length;
            }
            if (k.ColumnKeys is { Count: > 0 } columns)
            {
                int n = columns.Count;
                ColumnNames = new string[n];
                _columns = new MemoryHandle[n];
                _columnPointers = (nint*)NativeMemory.Alloc((nuint)n, (nuint)sizeof(nint));
                _columnLengths = (nuint*)NativeMemory.Alloc((nuint)n, (nuint)sizeof(nuint));
                int i = 0;
                foreach (var (name, key) in columns)
                {
                    ColumnNames[i] = name;
                    _columns[i] = key.Pin();
                    _columnPointers[i] = (nint)_columns[i].Pointer;
                    _columnLengths[i] = (nuint)key.Length;
                    i++;
                }
                ColumnCount = (nuint)n;
            }
        }

        public void Dispose()
        {
            if (FooterKey is not null && _footer.Pointer is null) NativeMemory.Free(FooterKey);
            _footer.Dispose();
            _aad.Dispose();
            if (_columns is not null) foreach (var c in _columns) c.Dispose();
            if (_columnPointers is not null) NativeMemory.Free(_columnPointers);
            if (_columnLengths is not null) NativeMemory.Free(_columnLengths);
        }
    }

    /// <summary>
    /// A handle the engine borrows for one call: referenced for the call's length, so that a handle disposed meanwhile
    /// (by another thread) fails to borrow instead of passing a freed pointer. A null handle is a null pointer.
    /// </summary>
    internal ref struct BorrowedHandle
    {
        private readonly SafeHandle? _handle;
        private readonly bool _added;

        public readonly IntPtr Pointer;

        public BorrowedHandle(SafeHandle? handle)
        {
            _handle = handle;
            if (handle is null) return;
            bool added = false;
            handle.DangerousAddRef(ref added);
            _added = added;
            Pointer = handle.DangerousGetHandle();
        }

        public readonly void Dispose()
        {
            if (_added) _handle!.DangerousRelease();
        }
    }
}
