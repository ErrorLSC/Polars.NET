use deltalake::Path;
use deltalake::kernel::{Action, Remove};
use polars::prelude::*;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ffi::c_char;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::delta::merge::{MergeStrategy, phase_execution, phase_execution_dv, phase_process_staging, phase_validation};
use crate::types::{ExprContext, LazyFrameContext};
use crate::utils::{ptr_to_str, ptr_to_vec_string};
use crate::delta::utils::RawCloudArgs;
use crate::delta::utils::build_delta_storage_options_map;
use crate::delta::utils::get_runtime;

const MAX_FULL_JOB_RETRIES: usize = 5;
// const MAX_PRUNING_CANDIDATES: usize = 100_000;


// =========================================================================
// 1. Action Types and Rules Definition
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MergeActionType {
    MatchedUpdate = 0,
    MatchedDelete = 1,
    NotMatchedInsert = 2,
    NotMatchedBySourceDelete = 3,
}

impl TryFrom<u8> for MergeActionType {
    type Error = PolarsError;

    fn try_from(value: u8) -> PolarsResult<Self> {
        match value {
            0 => Ok(MergeActionType::MatchedUpdate),
            1 => Ok(MergeActionType::MatchedDelete),
            2 => Ok(MergeActionType::NotMatchedInsert),
            3 => Ok(MergeActionType::NotMatchedBySourceDelete),
            _ => Err(PolarsError::ComputeError(
                format!("Unknown MergeActionType identifier: {}", value).into(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MergeActionRule {
    pub action_type: MergeActionType,
    pub condition: Expr,
}

// -------------------------------------------------------------------------
// Ordered Merge Context
// -------------------------------------------------------------------------
pub(crate) struct OrderedMergeContext {
    pub merge_keys: Vec<String>,
    pub table_url: url::Url,
    pub can_evolve: bool,
    pub strategy: crate::delta::merge::MergeStrategy, 
    pub rules: Vec<MergeActionRule>,                  
}

pub(crate) fn phase_planning_ordered(
    ctx: &OrderedMergeContext,
    target_lf: LazyFrame,
    mut source_lf: LazyFrame,
    target_schema: &Schema,
) -> PolarsResult<(LazyFrame, Option<LazyFrame>)> {
    
    // ---------------------------------------------------------
    // 1. Rename Source Columns
    // ---------------------------------------------------------
    let src_schema = source_lf.collect_schema()
        .map_err(|e| PolarsError::ComputeError(format!("Source schema error: {}", e).into()))?;
    
    let mut rename_old = Vec::new();
    let mut rename_new = Vec::new();
    let mut source_cols_set = HashSet::new();

    for name in src_schema.iter_names() {
        rename_old.push(name.as_str());
        rename_new.push(format!("{}_src_tmp", name)); 
        source_cols_set.insert(name.to_string());
    }
    let source_renamed = source_lf.rename(rename_old, rename_new, true);

    // ---------------------------------------------------------
    // 2. Full Outer Join
    // ---------------------------------------------------------
    let left_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(k)).collect();
    let right_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(&format!("{}_src_tmp", k))).collect();

    let join_args = JoinArgs {
        how: JoinType::Full,
        validation: JoinValidation::ManyToMany, 
        coalesce: JoinCoalesce::KeepColumns,    
        maintain_order: MaintainOrderJoin::None,
        ..Default::default() 
    };
    let joined_lf = target_lf.join(source_renamed, left_on, right_on, join_args);

    // ---------------------------------------------------------
    // 3. Define Action Constants & State
    // ---------------------------------------------------------
    let act_keep   = lit(0);
    let act_insert = lit(1);
    let act_update = lit(2);
    let act_delete = lit(3);
    let act_ignore = lit(4);

    let first_key = &ctx.merge_keys[0];
    let src_key_col = format!("{}_src_tmp", first_key);
    
    let src_exists = col(&src_key_col).is_not_null();
    let tgt_exists = col(first_key).is_not_null();

    let is_matched     = src_exists.clone().and(tgt_exists.clone());
    let is_source_only = src_exists.clone().and(tgt_exists.clone().not());
    let is_target_only = src_exists.not().and(tgt_exists);

    // ---------------------------------------------------------
    // 4. Dynamically Build WHEN-THEN Tree
    // ---------------------------------------------------------
    
    // Step A: Define Fallback
    // - New Data in Source if not fit Insert rule => Ignore
    // - Old Data in Target if not fin Delete/Update rule => Keep
    let mut action_expr = when(is_source_only.clone())
        .then(act_ignore.clone())
        .otherwise(act_keep.clone());

    // Step B: Reverse Fold Steps
    for rule in ctx.rules.iter().rev() {
        let (state_expr, result_act) = match rule.action_type {
            MergeActionType::MatchedUpdate => (is_matched.clone(), act_update.clone()),
            MergeActionType::MatchedDelete => (is_matched.clone(), act_delete.clone()),
            MergeActionType::NotMatchedInsert => (is_source_only.clone(), act_insert.clone()),
            MergeActionType::NotMatchedBySourceDelete => (is_target_only.clone(), act_delete.clone()),
        };

        // Full Conditions = (match stats) AND (user defined filters)
        let full_cond = state_expr.and(rule.condition.clone());

        // action_expr as otherwise (Else branch)
        action_expr = when(full_cond)
            .then(result_act)
            .otherwise(action_expr);
    }

    let action_expr = action_expr.alias("_merge_action");
    let lf_with_action = joined_lf.with_column(action_expr);

    // ---------------------------------------------------------
    // 5. Projection to output column
    // ---------------------------------------------------------
    let mut new_data_exprs = Vec::new();
    let mut output_cols = Vec::new();
    let mut seen = HashSet::new();

    // Collect Schema Evolution
    for name in target_schema.iter_names() {
        if name == "__row_index" || name == "__file_path" { continue; }
        output_cols.push(name.to_string());
        seen.insert(name.to_string());
    }
    for name in src_schema.iter_names() {
        if !seen.contains(name.as_str()) {
            output_cols.push(name.to_string());
        }
    }

    // Generate Select Exprs
    for col_name in output_cols {
        let src_col_alias = format!("{}_src_tmp", col_name);
        let has_source = source_cols_set.contains(&col_name);
        let has_target = target_schema.get_field(&col_name).is_some();

        let src_val = if has_source { col(&src_col_alias) } else { lit(NULL) };
        let tgt_val = if has_target { col(&col_name) } else { lit(NULL) };

        let col_expr = if has_source {
            when(col("_merge_action").eq(act_insert.clone()).or(col("_merge_action").eq(act_update.clone())))
            .then(src_val).otherwise(tgt_val)
        } else {
            when(col("_merge_action").eq(act_insert.clone()))
            .then(lit(NULL)).otherwise(tgt_val)
        };

        new_data_exprs.push(col_expr.alias(&col_name));
    }

    // ---------------------------------------------------------
    // 6. CoW vs MoR
    // ---------------------------------------------------------
    match ctx.strategy {
        crate::delta::merge::MergeStrategy::CopyOnWrite => {
            let processed_lf = lf_with_action
                .filter(col("_merge_action").neq(act_delete.clone()).and(col("_merge_action").neq(act_ignore)))
                .select(new_data_exprs);
            Ok((processed_lf, None)) 
        },
        crate::delta::merge::MergeStrategy::MergeOnRead => {
            let new_data_lf = lf_with_action.clone()
                .filter(col("_merge_action").eq(act_insert.clone()).or(col("_merge_action").eq(act_update.clone())))
                .select(new_data_exprs);

            let tombstones_lf = lf_with_action
                .filter(col("_merge_action").eq(act_delete.clone()).or(col("_merge_action").eq(act_update.clone())))
                .select([col("__file_path"), col("__row_index")]);

            Ok((new_data_lf, Some(tombstones_lf)))
        }
    }
}



// =========================================================================
// 2. FFI Entry Point for Ordered Merge
// =========================================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_merge_ordered(
    source_lf_ptr: *mut LazyFrameContext, 
    target_path_ptr: *const c_char,
    merge_keys_ptr: *const *const c_char,
    merge_keys_len: usize,
    
    // --- Actions ---
    action_types_ptr: *const u8,               
    action_exprs_ptr: *const *mut ExprContext, 
    actions_count: usize,                      
    
    // --- Schema & Cloud Args ---
    can_evolve: bool, 
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64, 
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        // Parse params
        let path_str = ptr_to_str(target_path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let merge_keys = unsafe { ptr_to_vec_string(merge_keys_ptr, merge_keys_len) };
        let table_url = crate::delta::utils::parse_table_url(path_str)?;
        
        if merge_keys.is_empty() {
             return Err(PolarsError::ComputeError("Merge keys cannot be empty".into()));
        }

        // Consume Source LazyFrame
        let source_lf_ctx = unsafe { Box::from_raw(source_lf_ptr) };
        let mut source_lf = source_lf_ctx.inner; 

        // Build Ordered Rule Vec
        let mut rules = Vec::with_capacity(actions_count);
        if actions_count > 0 && !action_types_ptr.is_null() && !action_exprs_ptr.is_null() {
            let types_slice = unsafe { std::slice::from_raw_parts(action_types_ptr, actions_count) };
            let exprs_slice = unsafe { std::slice::from_raw_parts(action_exprs_ptr, actions_count) };

            for i in 0..actions_count {
                let action_type = MergeActionType::try_from(types_slice[i])?;
                let expr = unsafe { *Box::from_raw(exprs_slice[i]) }.inner;
                
                rules.push(MergeActionRule { action_type, condition: expr });
            }
        }

        // Build Cloud Args 
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let cloud_args = RawCloudArgs {
            provider: cloud_provider, retries: cloud_retries, retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms, retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl, keys: cloud_keys, values: cloud_values, len: cloud_len,
        };

        // Init Runtime and Table
        let rt = get_runtime();
        let (mut table, partition_cols, mut target_schema) = rt.block_on(
            crate::delta::merge::phase_1_load_table(table_url.clone(), delta_storage_options)
        )?;
        
        let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
        let strategy = crate::delta::merge::MergeStrategy::determine(snapshot, false);

        // Build Ordered Context
        let ctx = OrderedMergeContext {
            table_url,
            merge_keys,
            can_evolve,
            strategy,
            rules, 
        };

        let mut source_lf_clone = source_lf.clone();
        phase_validation(&ctx.merge_keys, ctx.can_evolve, &mut source_lf_clone, &target_schema)?;

        let mut attempt = 0;
        
        loop {
            attempt += 1;
            if attempt > 1 {
                rt.block_on(async {
                    table.update_state().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                })?;
                let polars_schema = crate::delta::utils::get_polars_schema_from_delta(&table)?;
                target_schema = polars_schema.into();
            }

            let (src_parts, key_bounds, cands) = crate::delta::merge::phase_1_analyze_source(
                &ctx.merge_keys, &source_lf, &target_schema, &partition_cols
            )?;
            let candidate_files = rt.block_on(
                crate::delta::merge::phase_1_scan_and_prune(&table, &partition_cols, src_parts, key_bounds, cands, ctx.merge_keys.get(0))
            )?;

            let target_lf = crate::delta::merge::construct_target_lf(
                &ctx.table_url, 
                ctx.strategy, 
                &table, 
                &candidate_files, 
                &target_schema, 
                &cloud_args
            )?;

            let (new_data_lf, tombstones_lf_opt) = phase_planning_ordered(
                &ctx, target_lf, source_lf.clone(), &target_schema
            )?;

            let (staging_dir, write_id) = phase_execution(&ctx.table_url, new_data_lf, &partition_cols, &cloud_args)?;

            let staging_dir_for_commit = staging_dir.clone();

            // Calculate Tombstone DataFrame here in sync environment
            let tombstones_df_opt = match tombstones_lf_opt {
                Some(lf) => {
                    let df = lf
                        .group_by([col("__file_path")])
                        .agg([col("__row_index")])
                        .collect_with_engine(Engine::Streaming)?;
                    
                    if df.height() > 0 { Some(df) } else { None }
                },
                None => None,
            };
            // ---------------------------------------------------------------------
            // Phase 5: Commit (Try to finalize)
            // ---------------------------------------------------------------------

            let commit_result = rt.block_on(async {
                
                // Parse Staging Files to get Add(New Data) Actions
                let mut final_actions = phase_process_staging(&table, &staging_dir, &partition_cols, write_id).await?;

                // Handle old files
                match ctx.strategy {
                    MergeStrategy::CopyOnWrite => {
                        // CoW：Remove old files
                        for add in &candidate_files {
                            let remove = Remove {
                                path: add.path.clone(),
                                deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                                data_change: true,
                                extended_file_metadata: Some(true),
                                partition_values: Some(add.partition_values.clone()),
                                size: Some(add.size),
                                deletion_vector: add.deletion_vector.clone(),
                                ..Default::default()
                            };
                            final_actions.push(Action::Remove(remove));
                        }
                    },
                    MergeStrategy::MergeOnRead => {
                        // MoR：Generate DV then build Remove/Add Actions
                        if let Some(t_df) = tombstones_df_opt {
                            let dv_actions = phase_execution_dv(&ctx.table_url, &table, t_df, &candidate_files).await?;
                            final_actions.extend(dv_actions);
                        }
                    }
                }

                if !final_actions.is_empty() {
                    let src_schema = source_lf.collect_schema().unwrap();
                    let mut matched_preds = Vec::new();
                        let mut not_matched_preds = Vec::new();
                        let mut not_matched_by_source_preds = Vec::new();

                        for rule in &ctx.rules {
                            match rule.action_type {
                                MergeActionType::MatchedUpdate => {
                                    matched_preds.push(deltalake::protocol::MergePredicate { 
                                        predicate: Some("ordered_update_condition".into()), action_type: "UPDATE".into() 
                                    });
                                },
                                MergeActionType::MatchedDelete => {
                                    matched_preds.push(deltalake::protocol::MergePredicate { 
                                        predicate: Some("ordered_delete_condition".into()), action_type: "DELETE".into() 
                                    });
                                },
                                MergeActionType::NotMatchedInsert => {
                                    not_matched_preds.push(deltalake::protocol::MergePredicate { 
                                        predicate: Some("ordered_insert_condition".into()), action_type: "INSERT".into() 
                                    });
                                },
                                MergeActionType::NotMatchedBySourceDelete => {
                                    not_matched_by_source_preds.push(deltalake::protocol::MergePredicate { 
                                        predicate: Some("ordered_source_delete_condition".into()), action_type: "DELETE".into() 
                                    });
                                },
                            }
                        }

                        crate::delta::merge::phase_commit(
                            &mut table, final_actions, &staging_dir_for_commit, &src_schema, &target_schema,
                            &ctx.merge_keys, ctx.can_evolve,
                            matched_preds, not_matched_preds, not_matched_by_source_preds
                        ).await?;
                } else {
                     let object_store = table.object_store();
                     let _ = object_store.delete(&Path::from(staging_dir)).await;
                }
                Ok::<(), PolarsError>(())
            });

            // ---------------------------------------------------------------------
            // Error Handling & Retry Decision
            // ---------------------------------------------------------------------
            match commit_result {
                Ok(_) => {
                    break;
                },
                Err(e) => {
                    let err_msg = format!("{:?}", e);
                    
                    let is_conflict = err_msg.contains("Transaction") || 
                                      err_msg.contains("Conflict") || 
                                      err_msg.contains("VersionMismatch"); 

                    if is_conflict && attempt < MAX_FULL_JOB_RETRIES {
                        rt.block_on(async {
                            let object_store = table.object_store();
                            let _ = object_store.delete(&Path::from(staging_dir_for_commit)).await;
                        });
                        
                        let base_sleep = 50_u64.saturating_mul(2_u64.pow((attempt - 1) as u32));
                        
                        // Set cap as 1000ms
                        let capped_sleep = std::cmp::min(base_sleep, 1000);
                        
                        // Full Jitter
                        let mut rng = rand::rng();
                        let jitter_millis = rand::RngExt::random_range(&mut rng, (capped_sleep/2)..=(capped_sleep*3/2));

                        println!("[Delta-RS] Conflict! Backoff for {}ms (Attempt {})", jitter_millis, attempt);

                        std::thread::sleep(std::time::Duration::from_millis(jitter_millis));
                        
                        continue; 
                    } else {
                        return Err(e);
                    }
                }
            }
        } // End Loop

        Ok(())

    })
}