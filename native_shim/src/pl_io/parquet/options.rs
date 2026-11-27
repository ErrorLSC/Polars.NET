//! The Parquet scan and write options that do not fit in plain arguments: cast rules, hidden file prefixes,
//! key-value metadata, a custom Arrow schema, and Parquet modular encryption. Each side builds one handle that the
//! scan / sink functions borrow (a null handle means "none of these").

use polars::prelude::*;
use polars_io::parquet::encryption::{ParquetDecryption, ParquetEncryption};
use polars_io::parquet::write::KeyValueMetadata;
use polars_parquet::parquet::encryption::decrypt::FileDecryptionProperties;
use polars_parquet::parquet::encryption::encrypt::FileEncryptionProperties;
use polars_plan::dsl::{CastColumnsPolicy, ExtraColumnsPolicy, MissingColumnsPolicy};
use std::ffi::CStr;
use std::os::raw::c_char;

/// Options a Parquet scan takes beyond `ScanArgsParquet`.
pub struct ParquetScanExtrasContext {
    pub ignore_extra_columns: bool,
    pub cast_columns_policy: CastColumnsPolicy,
    pub hidden_file_prefix: Option<Arc<[PlSmallStr]>>,
    pub decryption: Option<ParquetDecryption>,
}

impl Default for ParquetScanExtrasContext {
    fn default() -> Self {
        Self {
            ignore_extra_columns: false,
            cast_columns_policy: CastColumnsPolicy::ERROR_ON_MISMATCH,
            hidden_file_prefix: None,
            decryption: None,
        }
    }
}

/// Options a Parquet write takes beyond compression, statistics and sizes.
#[derive(Default)]
pub struct ParquetWriteExtrasContext {
    pub key_value_metadata: Option<KeyValueMetadata>,
    pub arrow_schema: Option<polars_arrow::datatypes::ArrowSchemaRef>,
    pub encryption: Option<ParquetEncryption>,
}

/// The extras behind a borrowed handle, or the defaults for a null one.
pub(crate) fn scan_extras(ptr: *const ParquetScanExtrasContext) -> ParquetScanExtrasContext {
    match unsafe { ptr.as_ref() } {
        None => ParquetScanExtrasContext::default(),
        Some(e) => ParquetScanExtrasContext {
            ignore_extra_columns: e.ignore_extra_columns,
            cast_columns_policy: e.cast_columns_policy.clone(),
            hidden_file_prefix: e.hidden_file_prefix.clone(),
            decryption: e.decryption.clone(),
        },
    }
}

pub(crate) fn write_extras(ptr: *const ParquetWriteExtrasContext) -> ParquetWriteExtrasContext {
    match unsafe { ptr.as_ref() } {
        None => ParquetWriteExtrasContext::default(),
        Some(e) => ParquetWriteExtrasContext {
            key_value_metadata: e.key_value_metadata.clone(),
            arrow_schema: e.arrow_schema.clone(),
            encryption: e.encryption.clone(),
        },
    }
}

// Bits of `cast_flags`; one per `CastColumnsPolicy` switch (Python's ScanCastOptions).
const CAST_INTEGER_UPCAST: u32 = 1 << 0;
const CAST_INTEGER_TO_FLOAT: u32 = 1 << 1;
const CAST_FLOAT_UPCAST: u32 = 1 << 2;
const CAST_FLOAT_DOWNCAST: u32 = 1 << 3;
const CAST_DATETIME_NS_DOWNCAST: u32 = 1 << 4;
const CAST_DATETIME_US_DOWNCAST: u32 = 1 << 5;
const CAST_DATETIME_MS_UPCAST: u32 = 1 << 6;
const CAST_DATETIME_US_UPCAST: u32 = 1 << 7;
const CAST_DATETIME_CONVERT_TIMEZONE: u32 = 1 << 8;
const CAST_CATEGORICAL_TO_STRING: u32 = 1 << 9;
const CAST_INSERT_MISSING_STRUCT_FIELDS: u32 = 1 << 10;
const CAST_IGNORE_EXTRA_STRUCT_FIELDS: u32 = 1 << 11;

fn cast_policy(flags: u32) -> CastColumnsPolicy {
    let on = |bit: u32| flags & bit != 0;
    CastColumnsPolicy {
        integer_upcast: on(CAST_INTEGER_UPCAST),
        integer_to_float_cast: on(CAST_INTEGER_TO_FLOAT),
        float_upcast: on(CAST_FLOAT_UPCAST),
        float_downcast: on(CAST_FLOAT_DOWNCAST),
        datetime_nanoseconds_downcast: on(CAST_DATETIME_NS_DOWNCAST),
        datetime_microseconds_downcast: on(CAST_DATETIME_US_DOWNCAST),
        datetime_milliseconds_upcast: on(CAST_DATETIME_MS_UPCAST),
        datetime_microseconds_upcast: on(CAST_DATETIME_US_UPCAST),
        datetime_convert_timezone: on(CAST_DATETIME_CONVERT_TIMEZONE),
        categorical_to_string: on(CAST_CATEGORICAL_TO_STRING),
        missing_struct_fields: if on(CAST_INSERT_MISSING_STRUCT_FIELDS) { MissingColumnsPolicy::Insert } else { MissingColumnsPolicy::Raise },
        extra_struct_fields: if on(CAST_IGNORE_EXTRA_STRUCT_FIELDS) { ExtraColumnsPolicy::Ignore } else { ExtraColumnsPolicy::Raise },
        ..CastColumnsPolicy::ERROR_ON_MISMATCH
    }
}

/// UTF-8 strings, every one required (a column key must stay paired with its name).
unsafe fn strings(ptr: *const *const c_char, len: usize, what: &str) -> PolarsResult<Vec<String>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    polars_ensure!(!ptr.is_null(), ComputeError: "{what}: null array");
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice
        .iter()
        .map(|&p| {
            polars_ensure!(!p.is_null(), ComputeError: "{what}: null string");
            unsafe { CStr::from_ptr(p) }
                .to_str()
                .map(str::to_owned)
                .map_err(|_| polars_err!(ComputeError: "{what}: not UTF-8"))
        })
        .collect()
}

unsafe fn bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    if len == 0 { Vec::new() } else { unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec() }
}

/// The column keys as (dot-separated path, key) pairs.
unsafe fn column_keys(
    names: *const *const c_char,
    keys: *const *const u8,
    key_lens: *const usize,
    len: usize,
) -> PolarsResult<Vec<(String, Vec<u8>)>> {
    let names = unsafe { strings(names, len, "column key names") }?;
    if len == 0 {
        return Ok(Vec::new());
    }
    polars_ensure!(!keys.is_null() && !key_lens.is_null(), ComputeError: "column keys: null array");
    let keys = unsafe { std::slice::from_raw_parts(keys, len) };
    let lens = unsafe { std::slice::from_raw_parts(key_lens, len) };
    Ok(names
        .into_iter()
        .zip(keys.iter().zip(lens))
        .map(|(name, (&key, &n))| (name, unsafe { bytes(key, n) }))
        .collect())
}

fn parquet_err(e: impl std::fmt::Display) -> PolarsError {
    PolarsError::ComputeError(format!("parquet: {e}").into())
}

/// Builds the scan extras. `footer_key` null = no decryption; `aad_prefix` null = none supplied.
#[unsafe(no_mangle)]
pub extern "C" fn pl_parquet_scan_extras_new(
    ignore_extra_columns: bool,
    cast_flags: u32,
    hidden_prefixes: *const *const c_char,
    hidden_prefixes_len: usize,
    footer_key: *const u8,
    footer_key_len: usize,
    column_key_names: *const *const c_char,
    column_keys_ptr: *const *const u8,
    column_key_lens: *const usize,
    column_keys_len: usize,
    aad_prefix: *const u8,
    aad_prefix_len: usize,
    verify_footer_signature: bool,
) -> *mut ParquetScanExtrasContext {
    ffi_try!({
        let prefixes = unsafe { strings(hidden_prefixes, hidden_prefixes_len, "hidden file prefixes") }?;
        let hidden_file_prefix = (!prefixes.is_empty())
            .then(|| prefixes.into_iter().map(PlSmallStr::from).collect::<Arc<[_]>>());

        let decryption = if footer_key.is_null() {
            None
        } else {
            let mut builder = FileDecryptionProperties::builder(unsafe { bytes(footer_key, footer_key_len) });
            for (name, key) in unsafe { column_keys(column_key_names, column_keys_ptr, column_key_lens, column_keys_len) }? {
                builder = builder.with_column_key(&name, key);
            }
            if !aad_prefix.is_null() {
                builder = builder.with_aad_prefix(unsafe { bytes(aad_prefix, aad_prefix_len) });
            }
            if !verify_footer_signature {
                builder = builder.disable_footer_signature_verification();
            }
            Some(ParquetDecryption::from(builder.build().map_err(parquet_err)?))
        };

        Ok(Box::into_raw(Box::new(ParquetScanExtrasContext {
            ignore_extra_columns,
            cast_columns_policy: cast_policy(cast_flags),
            hidden_file_prefix,
            decryption,
        })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_parquet_scan_extras_free(ptr: *mut ParquetScanExtrasContext) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

/// Builds the write extras. `arrow_schema` is borrowed (the caller releases it); `footer_key` null = no encryption;
/// `aad_prefix` null = none.
#[unsafe(no_mangle)]
pub extern "C" fn pl_parquet_write_extras_new(
    metadata_keys: *const *const c_char,
    metadata_values: *const *const c_char,
    metadata_len: usize,
    arrow_schema: *const polars_arrow::ffi::ArrowSchema,
    footer_key: *const u8,
    footer_key_len: usize,
    column_key_names: *const *const c_char,
    column_keys_ptr: *const *const u8,
    column_key_lens: *const usize,
    column_keys_len: usize,
    plaintext_footer: bool,
    aad_prefix: *const u8,
    aad_prefix_len: usize,
    store_aad_prefix: bool,
) -> *mut ParquetWriteExtrasContext {
    ffi_try!({
        let keys = unsafe { strings(metadata_keys, metadata_len, "metadata keys") }?;
        let values = unsafe { strings(metadata_values, metadata_len, "metadata values") }?;
        let key_value_metadata = (metadata_len > 0)
            .then(|| KeyValueMetadata::from_static(keys.into_iter().zip(values).collect()));

        let arrow_schema = match unsafe { arrow_schema.as_ref() } {
            None => None,
            Some(c_schema) => {
                let field = unsafe { polars_arrow::ffi::import_field_from_c(c_schema) }?;
                let ArrowDataType::Struct(fields) = field.dtype else {
                    polars_bail!(ComputeError: "the Arrow schema must be a struct of the columns, got {:?}", field.dtype);
                };
                let mut schema = ArrowSchema::from_iter_check_duplicates(fields)?;
                if let Some(md) = field.metadata {
                    *schema.metadata_mut() = Arc::unwrap_or_clone(md);
                }
                Some(Arc::new(schema))
            },
        };

        let encryption = if footer_key.is_null() {
            None
        } else {
            let mut builder = FileEncryptionProperties::builder(unsafe { bytes(footer_key, footer_key_len) })
                .with_plaintext_footer(plaintext_footer);
            for (name, key) in unsafe { column_keys(column_key_names, column_keys_ptr, column_key_lens, column_keys_len) }? {
                builder = builder.with_column_key(&name, key);
            }
            if !aad_prefix.is_null() {
                builder = builder
                    .with_aad_prefix(unsafe { bytes(aad_prefix, aad_prefix_len) })
                    .with_aad_prefix_storage(store_aad_prefix);
            }
            Some(ParquetEncryption::from(builder.build().map_err(parquet_err)?))
        };

        Ok(Box::into_raw(Box::new(ParquetWriteExtrasContext { key_value_metadata, arrow_schema, encryption })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_parquet_write_extras_free(ptr: *mut ParquetWriteExtrasContext) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}
