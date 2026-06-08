use polars::prelude::*;
use rust_xlsxwriter::{Workbook, Format};
use chrono::{NaiveDate,DateTime};
use std::ffi::c_char;
use crate::utils::ptr_to_str;
use crate::types::DataFrameContext;

// ---------------------------------------------------------
// Native Excel Writer
// ---------------------------------------------------------
pub fn write_excel_native(
    df: &DataFrame, 
    path: &str, 
    sheet_name: Option<&str>,
    date_fmt_str: Option<&str>,     
    datetime_fmt_str: Option<&str>, 
) -> PolarsResult<()> {
    
    // Create Workbook
    let mut workbook = Workbook::new();
    
    // Add Sheet
    let worksheet = if let Some(name) = sheet_name {
        workbook.add_worksheet().set_name(name).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
    } else {
        workbook.add_worksheet()
    };

    // Prepare date&datetime format
    let d_fmt = date_fmt_str.unwrap_or("yyyy-mm-dd");
    let dt_fmt = datetime_fmt_str.unwrap_or("yyyy-mm-dd hh:mm:ss");

    let date_fmt = Format::new().set_num_format(d_fmt);
    let datetime_fmt = Format::new().set_num_format(dt_fmt);

    // Write header (Row 0)
    for (col_idx, name) in df.get_column_names().iter().enumerate() {
        worksheet.write_string(0, col_idx as u16, name.as_str())
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    }

    // Write data
    for (col_idx, series) in df.columns().iter().enumerate() {
        let col_u16 = col_idx as u16;

        match series.dtype() {
            // --- Safe Integers ---
            DataType::Int8 | DataType::Int16 | DataType::Int32 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
                let ca = series.cast(&DataType::Int64)?; 
                let ca_i64 = ca.i64()?;
                
                for (row_offset, opt_val) in ca_i64.iter().enumerate() {
                    if let Some(v) = opt_val {
                        worksheet.write_number((row_offset + 1) as u32, col_u16, v as f64)
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                    }
                }
            },

            // --- Unsafe Integers & Decimal ---
            DataType::Int64 | DataType::UInt64 | DataType::Int128 | DataType::UInt128 |
            DataType::Decimal(_, _) => {
                let s_str = series.cast(&DataType::String)?;
                let ca = s_str.str()?;
                
                for (row_offset, opt_val) in ca.iter().enumerate() {
                    if let Some(v) = opt_val {
                        worksheet.write_string((row_offset + 1) as u32, col_u16, v)
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                    }
                }
            },
            
            // --- Float ---
            DataType::Float32 | DataType::Float64 => {
                let ca = series.cast(&DataType::Float64)?;
                let ca_f64 = ca.f64()?;

                for (row_offset, opt_val) in ca_f64.iter().enumerate() {
                    if let Some(v) = opt_val {
                         worksheet.write_number((row_offset + 1) as u32, col_u16, v)
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                    }
                }
            },

            // --- String ---
            DataType::String => {
                let ca = series.str()?;
                for (row_offset, opt_val) in ca.iter().enumerate() {
                    if let Some(v) = opt_val {
                        worksheet.write_string((row_offset + 1) as u32, col_u16, v)
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                    }
                }
            },

            // --- Boolean ---
            DataType::Boolean => {
                let ca = series.bool()?;
                for (row_offset, opt_val) in ca.iter().enumerate() {
                    if let Some(v) = opt_val {
                        worksheet.write_boolean((row_offset + 1) as u32, col_u16, v)
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                    }
                }
            },

            // --- Date ---
            DataType::Date => {
                let ca = series.date()?;
                for (row_offset, opt_val) in ca.phys.iter().enumerate() {
                    if let Some(days) = opt_val {
                        if let Some(dt) = NaiveDate::from_ymd_opt(1970, 1, 1) {
                            let date_val = dt + chrono::Duration::days(days as i64);
                            
                            worksheet.write_datetime_with_format(
                                (row_offset + 1) as u32, 
                                col_u16, 
                                &date_val, 
                                &date_fmt
                            ).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                        }
                    }
                }
            },

            // --- Datetime ---
            DataType::Datetime(_, _) => {
                let ca = series.datetime()?; 
                for (row_offset, opt_val) in ca.phys.iter().enumerate() {
                    if let Some(micros) = opt_val {
                        if let Some(dt) = DateTime::from_timestamp_micros(micros).map(|d| d.naive_utc()) {
                            
                            worksheet.write_datetime_with_format(
                                (row_offset + 1) as u32, 
                                col_u16, 
                                &dt, 
                                &datetime_fmt
                            ).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                        }
                    }
                }
            },

            // Fallback: String
            _ => {
                if let Ok(s_str) = series.cast(&DataType::String) {
                     let ca = s_str.str()?;
                     for (row_offset, opt_val) in ca.iter().enumerate() {
                        if let Some(v) = opt_val {
                            worksheet.write_string((row_offset + 1) as u32, col_u16, v)
                                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                        }
                    }
                }
            }
        }
    }

    // Save File
    workbook.save(path).map_err(|e| PolarsError::ComputeError(format!("Failed to save excel: {}", e).into()))?;

    Ok(())
}

// ---------------------------------------------------------
// FFI Export
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_write_excel(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    sheet_name_ptr: *const c_char,
    date_fmt_ptr: *const c_char,
    datetime_fmt_ptr: *const c_char, 
) {
    ffi_try_void!({
        // Ptr check
        if df_ptr.is_null() {
            return Err(PolarsError::ComputeError("DataFrame handle is null".into()));
        }
        let df = unsafe { &(*df_ptr).df };

        // String conversion
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(format!("Invalid path string: {}", e).into()))?;

        let sheet_name = if !sheet_name_ptr.is_null() {
            Some(ptr_to_str(sheet_name_ptr)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid sheet name: {}", e).into()))?)
        } else {
            None
        };

        let date_fmt = if !date_fmt_ptr.is_null() {
            Some(ptr_to_str(date_fmt_ptr)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid date format: {}", e).into()))?)
        } else {
            None
        };

        let datetime_fmt = if !datetime_fmt_ptr.is_null() {
            Some(ptr_to_str(datetime_fmt_ptr)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid datetime format: {}", e).into()))?)
        } else {
            None
        };

        // Write Excel
        write_excel_native(df, path, sheet_name, date_fmt, datetime_fmt)
    })
}