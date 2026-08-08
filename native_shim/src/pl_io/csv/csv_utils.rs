use polars::prelude::{CsvEncoding, QuoteStyle};
use polars::prelude::*;
use polars_io::cloud::CloudOptions;
use polars_io::RowIndex;
use std::ffi::CStr;
use std::os::raw::c_char;
use crate::types::SchemaContext;
use crate::utils::ptr_to_vec_string;
use std::num::NonZeroUsize;
use crate::utils::{map_external_compression, ptr_to_opt_pl_str, ptr_to_pl_str};

#[inline]
pub(crate) fn map_csv_encoding(encoding: u8) -> CsvEncoding {
    match encoding {
        0 => CsvEncoding::Utf8,
        1 => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    }
}

#[inline]
pub(crate) fn map_quote_style(code: u8) -> QuoteStyle {
    match code {
        0 => QuoteStyle::Always,
        1 => QuoteStyle::Necessary,
        2 => QuoteStyle::NonNumeric,
        3 => QuoteStyle::Never,
        _ => QuoteStyle::Necessary, // Default
    }
}

#[inline]
pub(crate) unsafe fn apply_scan_csv_options(
    mut reader: LazyCsvReader,
    // --- 1. Core Configs ---
    has_header: bool,
    separator: u8,
    quote_char: u8,           
    eol_char: u8,             
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    cache: bool,
    glob: bool,
    rechunk: bool,
    raise_if_empty: bool,

    // --- 2. Sizes & Threading ---
    skip_rows: usize,
    skip_rows_after_header: usize,
    skip_lines: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,
    infer_schema_files: usize,
    n_threads_ptr: *const usize,
    chunk_size: usize,

    // --- 3. Row Index & Path ---
    row_index_name: *const c_char,
    row_index_offset: usize,
    include_file_paths: *const c_char,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext,
    dtype_override_ptr: *mut SchemaContext,
    encoding: u8, 
    
    // --- 5. Advanced Parsing ---
    null_values_ptr: *const *const c_char, 
    null_values_len: usize,                
    missing_is_null: bool,                 
    comment_prefix: *const c_char,         
    decimal_comma: bool,
    truncate_ragged_lines: bool,
    
    // --- 6. Cloud ---
    cloud_options: Option<CloudOptions>
) -> LazyCsvReader {
    
    // Core
    reader = reader
        .with_has_header(has_header)
        .with_separator(separator)
        .with_quote_char(if quote_char == 0 { None } else { Some(quote_char) }) 
        .with_eol_char(eol_char)                                        
        .with_ignore_errors(ignore_errors)
        .with_try_parse_dates(try_parse_dates)
        .with_low_memory(low_memory)
        .with_cache(cache)
        .with_glob(glob)
        .with_rechunk(rechunk)
        .with_raise_if_empty(raise_if_empty)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_skip_lines(skip_lines)
        .with_missing_is_null(missing_is_null)
        .with_decimal_comma(decimal_comma)
        .with_truncate_ragged_lines(truncate_ragged_lines)
        .with_encoding(map_csv_encoding(encoding));

    if chunk_size != 0 {
        reader = reader.with_chunk_size(chunk_size);
    }

    if let Some(non_zero_files) = std::num::NonZeroUsize::new(infer_schema_files) {
        reader = reader.with_infer_schema_files(non_zero_files);
    }
    
    if !n_rows_ptr.is_null() {
        reader = reader.with_n_rows(Some(unsafe { *n_rows_ptr }));
    }
    
    if !infer_schema_len_ptr.is_null() {
        reader = reader.with_infer_schema_length(Some(unsafe { *infer_schema_len_ptr }));
    }

    if !n_threads_ptr.is_null() {
        reader = reader.with_n_threads(Some(unsafe { *n_threads_ptr }));
    }

    if !dtype_override_ptr.is_null() {
        let override_schema = unsafe {(*dtype_override_ptr ).schema.clone()};
        reader = reader.with_dtype_overwrite(Some(override_schema));
    }
    if !schema_ptr.is_null() {
        let schema = unsafe { (*schema_ptr).schema.clone() };
        
        reader = reader.with_schema(Some(schema));
    }

    if !row_index_name.is_null() {
        let name_cow = unsafe { CStr::from_ptr(row_index_name).to_string_lossy() };
        reader = reader.with_row_index(Some(RowIndex {
            name: PlSmallStr::from_str(name_cow.as_ref()),
            offset: row_index_offset as u32,
        }));
    }

    if !include_file_paths.is_null() {
        let path_col = unsafe { CStr::from_ptr(include_file_paths).to_string_lossy() };
        reader = reader.with_include_file_paths(Some(PlSmallStr::from_str(path_col.as_ref())));
    }

    if !null_values_ptr.is_null() && null_values_len > 0 {
        let vec_str = unsafe { ptr_to_vec_string(null_values_ptr, null_values_len) };
        if !vec_str.is_empty() {
            let small_strs: Vec<PlSmallStr> = vec_str.into_iter()
                .map(|s| PlSmallStr::from_str(&s))
                .collect();
            reader = reader.with_null_values(Some(NullValues::AllColumns(small_strs)));
        }
    }

    if !comment_prefix.is_null() {
        let s = unsafe { CStr::from_ptr(comment_prefix).to_string_lossy() };
        reader = reader.with_comment_prefix(Some(PlSmallStr::from_str(s.as_ref())));
    }

    if let Some(opts) = cloud_options {
        reader = reader.with_cloud_options(Some(opts));
    }

    reader
}

pub(crate) unsafe fn build_serialize_options(
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32, // -1: None, 0: False, 1: True
    float_precision: i32,  // -1: None, >=0: Precision
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    quote_style_code: u8,
) -> SerializeOptions {
    
    // Float Scientific (Option<bool>)
    let float_scientific = match float_scientific {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };

    // Float Precision (Option<usize>)
    let float_precision = if float_precision >= 0 {
        Some(float_precision as usize)
    } else {
        None
    };

    unsafe {
    SerializeOptions {
        date_format: ptr_to_opt_pl_str(date_format_ptr),
        time_format: ptr_to_opt_pl_str(time_format_ptr),
        datetime_format: ptr_to_opt_pl_str(datetime_format_ptr),
        float_scientific,
        float_precision,
        decimal_comma,
        separator,
        quote_char,
        null: ptr_to_pl_str(null_value_ptr, ""), 
        line_terminator: ptr_to_pl_str(line_terminator_ptr, "\n"), 
        quote_style: map_quote_style(quote_style_code),
    }}
}

pub(crate) unsafe fn build_csv_writer_options(
    // --- CsvWriterOptions Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // --- ExternalCompression Params ---
    compression_code: u8,
    compression_level: i32,
    // --- SerializeOptions Params ---
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
) -> CsvWriterOptions {
    
    // SerializeOptions
    let serialize_options = unsafe { build_serialize_options(
        date_format, time_format, datetime_format,
        float_scientific, float_precision, decimal_comma,
        separator, quote_char, null_value, line_terminator, quote_style
    )};

    // Compression
    let compression = map_external_compression(compression_code, compression_level);

    // Batch Size
    let batch_size = NonZeroUsize::new(batch_size).unwrap_or(NonZeroUsize::new(1024).unwrap());

    CsvWriterOptions {
        include_bom,
        compression,
        check_extension, 
        include_header,
        batch_size,
        serialize_options: Arc::new(serialize_options), 
    }
}