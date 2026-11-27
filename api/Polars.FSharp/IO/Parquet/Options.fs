namespace Polars.FSharp

open System
open Polars.NET.Core
open Polars.NET.Core.Wrappers

/// <summary>Casts a scan may apply to integer columns.</summary>
[<Flags>]
type IntegerCasts =
    /// <summary>A type mismatch raises.</summary>
    | None = 0
    /// <summary>Lossless widening to a larger integer type (and widening a decimal's precision at the same scale).</summary>
    | Upcast = 1
    /// <summary>Integers may be read as floats.</summary>
    | ToFloat = 2

/// <summary>Casts a scan may apply to float columns.</summary>
[<Flags>]
type FloatCasts =
    /// <summary>A type mismatch raises.</summary>
    | None = 0
    /// <summary><c>Float32</c> may be read as <c>Float64</c>.</summary>
    | Upcast = 1
    /// <summary><c>Float64</c> may be read as <c>Float32</c>.</summary>
    | Downcast = 2

/// <summary>Casts a scan may apply to datetime columns.</summary>
[<Flags>]
type DatetimeCasts =
    /// <summary>A type mismatch raises.</summary>
    | None = 0
    /// <summary>Nanoseconds may be cut to any coarser unit (as Spark's INT96 timestamps need).</summary>
    | NanosecondDowncast = 1
    /// <summary>Microseconds may be cut to milliseconds.</summary>
    | MicrosecondDowncast = 2
    /// <summary>Microseconds may be read as nanoseconds.</summary>
    | MicrosecondUpcast = 4
    /// <summary>Milliseconds may be read as microseconds or nanoseconds.</summary>
    | MillisecondUpcast = 8
    /// <summary>Datetimes may be converted to the schema's time zone.</summary>
    | ConvertTimeZone = 16
    /// <summary>Every coarsening: NanosecondDowncast and MicrosecondDowncast.</summary>
    | Downcast = 3
    /// <summary>Every refinement: MillisecondUpcast and MicrosecondUpcast.</summary>
    | Upcast = 12

/// <summary>
/// The casts a scan may apply when a file's types differ from the schema it is read with (the given schema, or the
/// first file's). Every cast is off by default: a mismatch raises. Unstable in the engine.
/// </summary>
type ScanCastOptions
    (
        ?integers: IntegerCasts,
        ?floats: FloatCasts,
        ?datetimes: DatetimeCasts,
        ?insertMissingStructFields: bool,
        ?ignoreExtraStructFields: bool,
        ?categoricalToString: bool
    ) =
    member _.Integers = defaultArg integers IntegerCasts.None
    member _.Floats = defaultArg floats FloatCasts.None
    member _.Datetimes = defaultArg datetimes DatetimeCasts.None
    /// <summary>Struct fields of the schema that a file's struct lacks are filled with nulls.</summary>
    member _.InsertMissingStructFields = defaultArg insertMissingStructFields false
    /// <summary>Struct fields of a file that the schema does not name are dropped.</summary>
    member _.IgnoreExtraStructFields = defaultArg ignoreExtraStructFields false
    /// <summary>A categorical column read where the schema says String is cast to String.</summary>
    member _.CategoricalToString = defaultArg categoricalToString false

    member internal this.ToNative() =
        let on (set: bool) flag = if set then flag else PlScanCastFlags.None
        on (this.Integers.HasFlag IntegerCasts.Upcast) PlScanCastFlags.IntegerUpcast
        ||| on (this.Integers.HasFlag IntegerCasts.ToFloat) PlScanCastFlags.IntegerToFloat
        ||| on (this.Floats.HasFlag FloatCasts.Upcast) PlScanCastFlags.FloatUpcast
        ||| on (this.Floats.HasFlag FloatCasts.Downcast) PlScanCastFlags.FloatDowncast
        ||| on (this.Datetimes.HasFlag DatetimeCasts.NanosecondDowncast) PlScanCastFlags.DatetimeNanosecondDowncast
        ||| on (this.Datetimes.HasFlag DatetimeCasts.MicrosecondDowncast) PlScanCastFlags.DatetimeMicrosecondDowncast
        ||| on (this.Datetimes.HasFlag DatetimeCasts.MillisecondUpcast) PlScanCastFlags.DatetimeMillisecondUpcast
        ||| on (this.Datetimes.HasFlag DatetimeCasts.MicrosecondUpcast) PlScanCastFlags.DatetimeMicrosecondUpcast
        ||| on (this.Datetimes.HasFlag DatetimeCasts.ConvertTimeZone) PlScanCastFlags.DatetimeConvertTimeZone
        ||| on this.InsertMissingStructFields PlScanCastFlags.InsertMissingStructFields
        ||| on this.IgnoreExtraStructFields PlScanCastFlags.IgnoreExtraStructFields
        ||| on this.CategoricalToString PlScanCastFlags.CategoricalToString

module internal ParquetKeyMaterial =
    let columnKeys (keys: Map<string, byte[]> option) : Collections.Generic.IReadOnlyDictionary<string, ReadOnlyMemory<byte>> =
        match keys with
        | Some m when not m.IsEmpty ->
            m |> Map.map (fun _ (k: byte[]) -> ReadOnlyMemory<byte>(k)) :> Collections.Generic.IReadOnlyDictionary<_, _>
        | _ -> null

    let aad (prefix: byte[] option) : Nullable<ReadOnlyMemory<byte>> =
        match prefix with
        | Some p -> Nullable(ReadOnlyMemory<byte>(p))
        | None -> Nullable()

/// <summary>
/// How Parquet files are encrypted when written (Parquet modular encryption, AES-GCM), with keys given directly.
/// Without column keys the footer and every column are encrypted with the footer key (uniform encryption); with them
/// only those columns are encrypted, each with its own key (by dot-separated Parquet path), the others are written in
/// plain. The engine takes 16-byte (AES-128) and 32-byte (AES-256) keys, copies them into native memory for the write
/// and does not wipe them. Experimental; keys managed by a KMS are not supported.
/// </summary>
/// <param name="footerKey">The key of the footer, and of every column without its own key.</param>
/// <param name="columnKeys">The columns to encrypt, each with a key of its own, by dot-separated Parquet path.</param>
/// <param name="plaintextFooter">Leave the footer readable without a key (it is signed instead).</param>
/// <param name="aadPrefix">Additional authenticated data that binds the file to its context.</param>
/// <param name="storeAadPrefix">Store the AAD prefix in the file; when false (the default) readers must supply it.</param>
type ParquetEncryption
    (
        footerKey: byte[],
        ?columnKeys: Map<string, byte[]>,
        ?plaintextFooter: bool,
        ?aadPrefix: byte[],
        ?storeAadPrefix: bool
    ) =
    member _.FooterKey = footerKey
    member _.ColumnKeys = defaultArg columnKeys Map.empty
    member _.PlaintextFooter = defaultArg plaintextFooter false
    member _.AadPrefix = aadPrefix
    member _.StoreAadPrefix = defaultArg storeAadPrefix false
    /// <summary>Never shows the keys.</summary>
    override this.ToString() =
        $"ParquetEncryption(footer key {footerKey.Length} bytes, {this.ColumnKeys.Count} column keys)"
    member internal _.Keys =
        ParquetKeys(ReadOnlyMemory<byte>(footerKey), ParquetKeyMaterial.columnKeys columnKeys, ParquetKeyMaterial.aad aadPrefix)

/// <summary>
/// The keys to read Parquet files written with Parquet modular encryption (AES-GCM). A column encrypted with a key
/// of its own that is not given can still be left out of the read; reading it fails, naming the column. The engine
/// takes 16- and 32-byte keys and reads AES_GCM_V1 files (AES_GCM_CTR_V1 is refused). Experimental.
/// </summary>
/// <param name="footerKey">The key of the footer, and of every column without its own key.</param>
/// <param name="columnKeys">Keys of columns encrypted with a key of their own, by dot-separated Parquet path.</param>
/// <param name="aadPrefix">The AAD prefix the file was written with, when the file does not store it.</param>
/// <param name="verifyFooterSignature">Check the signature of a plaintext footer (default true).</param>
type ParquetDecryption
    (
        footerKey: byte[],
        ?columnKeys: Map<string, byte[]>,
        ?aadPrefix: byte[],
        ?verifyFooterSignature: bool
    ) =
    member _.FooterKey = footerKey
    member _.ColumnKeys = defaultArg columnKeys Map.empty
    member _.AadPrefix = aadPrefix
    member _.VerifyFooterSignature = defaultArg verifyFooterSignature true
    /// <summary>Never shows the keys.</summary>
    override this.ToString() =
        $"ParquetDecryption(footer key {footerKey.Length} bytes, {this.ColumnKeys.Count} column keys)"
    member internal _.Keys =
        ParquetKeys(ReadOnlyMemory<byte>(footerKey), ParquetKeyMaterial.columnKeys columnKeys, ParquetKeyMaterial.aad aadPrefix)

/// <summary>Builds the engine's Parquet option handles from the public options.</summary>
module internal ParquetNativeOptions =
    let scan (ignoreExtraColumns: bool option) (castOptions: ScanCastOptions option) (hiddenFilePrefixes: seq<string> option) (decryption: ParquetDecryption option) =
        PolarsWrapper.ParquetScanExtras(
            defaultArg ignoreExtraColumns false,
            (match castOptions with Some c -> c.ToNative() | None -> PlScanCastFlags.None),
            (match hiddenFilePrefixes with Some p -> Seq.toArray p | None -> null),
            (match decryption with Some d -> Nullable d.Keys | None -> Nullable()),
            (match decryption with Some d -> d.VerifyFooterSignature | None -> true))

    let write (metadata: Map<string, string> option) (arrowSchema: Apache.Arrow.Schema option) (encryption: ParquetEncryption option) =
        PolarsWrapper.ParquetWriteExtras(
            (match metadata with Some m when not m.IsEmpty -> m :> Collections.Generic.IReadOnlyDictionary<_, _> | _ -> null),
            Option.toObj arrowSchema,
            (match encryption with Some e -> Nullable e.Keys | None -> Nullable()),
            (match encryption with Some e -> e.PlaintextFooter | None -> false),
            (match encryption with Some e -> e.StoreAadPrefix | None -> false))
