use polars::prelude::*;
use std::sync::Arc;
use deltalake::kernel::{DeletionVectorDescriptor, StorageType};
use deltalake::{ObjectStore, Path};
use deltalake::logstore::object_store::ObjectStoreExt;
use polars::error::{PolarsError, PolarsResult};
use roaring::RoaringBitmap;
use uuid::Uuid;

/// Read and Parse Deletion Vector
/// 
/// Delta Lake Protocol for Deletion Vector：
/// - Inline (i): Z85 encoded in delta_log
/// - UuidRelativePath (u): Based on UUID generated file name, under Table Root
/// - AbsolutePath (p)
pub async fn read_deletion_vector(
    object_store: Arc<dyn ObjectStore>,
    descriptor: &DeletionVectorDescriptor,
    table_root: &Path,
) -> PolarsResult<RoaringBitmap> {
    
    match descriptor.storage_type {
        // =========================================================
        // Case 1: Inline 'i'
        // <base85 encoded bytes>
        // =========================================================
        StorageType::Inline => {
            // Z85 Decode
            let raw_bytes = z85::decode(&descriptor.path_or_inline_dv)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to Z85 decode inline DV: {}", e).into()))?;

            // 2. Deserialize
            let bitmap = RoaringBitmap::deserialize_from(&raw_bytes[..])
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize inline DV: {}", e).into()))?;

            Ok(bitmap)
        },

        // =========================================================
        // Case 2: UUID Relative Path 'u'
        // <random prefix><base85 encoded uuid>
        // =========================================================
        StorageType::UuidRelativePath => {
            // Reconstruct Filename
            // Last 20 Chars -> Z85 Decode -> 16 bytes -> UUID
            // Filename standard：deletion_vector_{uuid}.bin
            
            let encoded_str = &descriptor.path_or_inline_dv;
            if encoded_str.len() < 20 {
                 return Err(PolarsError::ComputeError(
                    format!("Invalid DV format 'u': string length too short '{}'", encoded_str).into()
                ));
            }

            // Last 20 Chars (Base85 UUID )
            let uuid_part = &encoded_str[encoded_str.len() - 20..];
            
            // Decode as UUID Bytes
            let uuid_bytes = z85::decode(uuid_part)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to Z85 decode DV UUID: {}", e).into()))?;
            
            // Build UUID Object
            let uuid = Uuid::from_slice(&uuid_bytes)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid DV UUID bytes: {}", e).into()))?;

            // Format Filename
            let file_name = format!("deletion_vector_{}.bin", uuid);
            
            // Format full path: table_root + file_name
            let file_path = table_root.clone().join(file_name);

            // Calc read range
            let offset = descriptor.offset.unwrap_or(0) as u64;
            let size = descriptor.size_in_bytes as u64;
            let range = offset..(offset + size);

            // read async
            let raw_bytes = object_store.get_range(&file_path, range).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to read DV file '{}': {}", file_path, e).into()))?;

            // Deserialize
            let bitmap = RoaringBitmap::deserialize_from(&raw_bytes[..])
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize file DV: {}", e).into()))?;

            Ok(bitmap)
        },

        // =========================================================
        // Case 3: Absolute Path 'p'
        // <absolute path>
        // =========================================================
        StorageType::AbsolutePath => {
            let file_path = Path::from(descriptor.path_or_inline_dv.as_str());
            
            let offset = descriptor.offset.unwrap_or(0) as u64;
            let size = descriptor.size_in_bytes as u64;
            let range = offset..(offset + size);

            let raw_bytes = object_store.get_range(&file_path, range).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to read absolute DV file: {}", e).into()))?;

            let bitmap = RoaringBitmap::deserialize_from(&raw_bytes[..])
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize absolute DV: {}", e).into()))?;

            Ok(bitmap)
        }
    }
}

/// Apply Deletion Vector to LazyFrame
///
/// Logic:
/// Convert RoaringBitmap to Polars Series (UInt32)。
/// Add temp row index column to LazyFrame
/// Filter index in Bitmap
/// Drop temp index column
pub fn apply_deletion_vector(
    lf: LazyFrame,
    bitmap: RoaringBitmap
) -> PolarsResult<LazyFrame> {
    
    // 0. FastPath：Return original lf
    if bitmap.is_empty() {
        return Ok(lf);
    }

    // 1. Convert RoaringBitmap to Vec<u32>
    let deleted_indices: Vec<u32> = bitmap.iter().collect();

    // 2. Build Polars Series
    let del_series = Series::new("deleted_idx".into(), deleted_indices);

    // 3. Build Expr
    let lf_filtered = lf
        .with_row_index("row_nr", None) 
        .filter(
            col("row_nr").is_in(lit(del_series).implode(false),false).not()
        )
        .drop(Selector::ByName {
            names: Arc::from([PlSmallStr::from_static("row_nr")]),
            strict: true 
        });

    Ok(lf_filtered)
}