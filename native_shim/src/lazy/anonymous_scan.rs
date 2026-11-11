//! Replaces anonymous scans (the C# stream scans behind ScanDatabase, ScanEnumerable, ScanRecordBatches, ...) before
//! a query runs on the streaming engine, which cannot execute them (polars-stream 0.55: `todo!("AnonymousScan")`).
//!
//! * [`AnonymousScanMode::Materialize`] reads the scan into memory and scans that DataFrame (what upstream Polars
//!   does too: it lowers the anonymous scan's subtree to the in-memory engine).
//! * [`AnonymousScanMode::Spool`] writes a C# stream to an anonymous temporary Arrow IPC file batch by batch and scans
//!   the file, so a sink runs fully streamed with memory bounded by a batch.
//!
//! Each scan is read once per execution, even when several plans (collect_all) or several branches (a self-join)
//! share it: a single-use IDataReader cannot be read twice.

use std::collections::HashMap;
use std::sync::Arc;

use polars::prelude::*;
use polars_plan::dsl::{DslPlan, FileScanDsl};
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};

use crate::pl_io::arrow::scan::{CSharpStreamScanner, scan_spooled_ipc};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AnonymousScanMode {
    Materialize,
    Spool,
}

impl AnonymousScanMode {
    /// The mode a collect of the plan with the engine needs: a plan that ends in a sink streams its rows out, so a
    /// C# stream is spooled; any other result is in memory anyway. None for the in-memory engine, which runs
    /// anonymous scans itself.
    pub(crate) fn for_collect(engine: Engine, plan: &DslPlan) -> Option<Self> {
        match engine {
            Engine::InMemory => None,
            _ if is_sink(plan) => Some(Self::Spool),
            _ => Some(Self::Materialize),
        }
    }

    /// One mode for plans collected together (they may share a source): spool only when every plan ends in a sink.
    pub(crate) fn for_collect_all(engine: Engine, plans: &[DslPlan]) -> Option<Self> {
        match engine {
            Engine::InMemory => None,
            _ if plans.iter().all(is_sink) => Some(Self::Spool),
            _ => Some(Self::Materialize),
        }
    }
}

/// The frame with its anonymous scans replaced (unchanged, at no cost, when it has none).
pub(crate) fn resolve_anonymous_scans(lf: LazyFrame, mode: AnonymousScanMode) -> PolarsResult<LazyFrame> {
    if !has_anonymous_scan(&lf.logical_plan) {
        return Ok(lf);
    }
    let opt_flags = lf.get_current_optimizations();
    let mut rewriter = Rewriter::new(mode);
    let plan = rewriter.plan(&lf.logical_plan)?.unwrap_or_else(|| lf.logical_plan.clone());
    // A fresh frame: the old one's cached arena holds the IR of the old plan.
    Ok(LazyFrame::from(plan).with_optimizations(opt_flags))
}

/// The plans with their anonymous scans replaced, each scan read once across all of them.
pub(crate) fn resolve_anonymous_scans_in_plans(plans: Vec<DslPlan>, mode: AnonymousScanMode) -> PolarsResult<Vec<DslPlan>> {
    if !plans.iter().any(has_anonymous_scan) {
        return Ok(plans);
    }
    let mut rewriter = Rewriter::new(mode);
    plans
        .into_iter()
        .map(|plan| Ok(rewriter.plan(&plan)?.unwrap_or(plan)))
        .collect()
}

fn is_sink(plan: &DslPlan) -> bool {
    match plan {
        DslPlan::Sink { .. } | DslPlan::SinkMultiple { .. } => true,
        DslPlan::IR { dsl, .. } => is_sink(dsl),
        _ => false,
    }
}

fn has_anonymous_scan(plan: &DslPlan) -> bool {
    plan.into_iter().any(|node| {
        matches!(node, DslPlan::Scan { scan_type, .. } if matches!(scan_type.as_ref(), FileScanDsl::Anonymous { .. }))
    })
}

struct Rewriter {
    mode: AnonymousScanMode,
    /// Replacement plan per scan function (by pointer).
    scans: HashMap<usize, DslPlan>,
    /// Rewritten shared inputs (by pointer), so a subplan shared between branches stays shared.
    inputs: HashMap<usize, Option<Arc<DslPlan>>>,
}

impl Rewriter {
    fn new(mode: AnonymousScanMode) -> Self {
        Self { mode, scans: HashMap::new(), inputs: HashMap::new() }
    }

    fn arc(&mut self, input: &Arc<DslPlan>) -> PolarsResult<Option<Arc<DslPlan>>> {
        let key = Arc::as_ptr(input) as usize;
        if let Some(done) = self.inputs.get(&key) {
            return Ok(done.clone());
        }
        let done = self.plan(input)?.map(Arc::new);
        self.inputs.insert(key, done.clone());
        Ok(done)
    }

    fn vec(&mut self, inputs: &[DslPlan]) -> PolarsResult<Option<Vec<DslPlan>>> {
        let rewritten = inputs.iter().map(|p| self.plan(p)).collect::<PolarsResult<Vec<_>>>()?;
        if rewritten.iter().all(Option::is_none) {
            return Ok(None);
        }
        Ok(Some(rewritten.into_iter().zip(inputs).map(|(new, old)| new.unwrap_or_else(|| old.clone())).collect()))
    }

    fn scan(&mut self, function: &Arc<dyn AnonymousScan>) -> PolarsResult<DslPlan> {
        let key = Arc::as_ptr(function) as *const () as usize;
        if let Some(done) = self.scans.get(&key) {
            return Ok(done.clone());
        }
        let csharp = function.as_any().downcast_ref::<CSharpStreamScanner>();
        let replacement = match (self.mode, csharp) {
            (AnonymousScanMode::Spool, Some(scanner)) => scan_spooled_ipc(scanner.spool_to_ipc()?)?.logical_plan,
            _ => {
                let schema = function.schema(None)?;
                let df = function.scan(AnonymousScanArgs {
                    n_rows: None,
                    with_columns: None,
                    schema,
                    output_schema: None,
                    predicate: None,
                })?;
                let schema = df.schema().clone();
                DslPlan::DataFrameScan { df: Arc::new(df), schema }
            },
        };
        self.scans.insert(key, replacement.clone());
        Ok(replacement)
    }

    /// The plan with its anonymous scans replaced; None when it has none. The match is exhaustive on purpose: a
    /// Polars upgrade that adds a plan node must decide how to walk it.
    fn plan(&mut self, plan: &DslPlan) -> PolarsResult<Option<DslPlan>> {
        use DslPlan::*;
        macro_rules! one {
            ($input:expr, |$new:ident| $rebuild:expr) => {
                match self.arc($input)? {
                    Some($new) => Some($rebuild),
                    None => None,
                }
            };
        }
        macro_rules! two {
            ($a:expr, $b:expr, |$na:ident, $nb:ident| $rebuild:expr) => {{
                let (ra, rb) = (self.arc($a)?, self.arc($b)?);
                if ra.is_none() && rb.is_none() {
                    None
                } else {
                    let $na = ra.unwrap_or_else(|| $a.clone());
                    let $nb = rb.unwrap_or_else(|| $b.clone());
                    Some($rebuild)
                }
            }};
        }
        Ok(match plan {
            Scan { scan_type, .. } => match scan_type.as_ref() {
                FileScanDsl::Anonymous { function, .. } => Some(self.scan(function)?),
                _ => None,
            },
            DataFrameScan { .. } => None,
            Filter { input, predicate } => one!(input, |input| Filter { input, predicate: predicate.clone() }),
            Cache { input, id } => one!(input, |input| Cache { input, id: *id }),
            Select { expr, input, options } => {
                one!(input, |input| Select { expr: expr.clone(), input, options: options.clone() })
            },
            GroupBy { input, keys, predicates, aggs, maintain_order, options, apply } => one!(input, |input| GroupBy {
                input,
                keys: keys.clone(),
                predicates: predicates.clone(),
                aggs: aggs.clone(),
                maintain_order: *maintain_order,
                options: options.clone(),
                apply: apply.clone(),
            }),
            Join { input_left, input_right, left_on, right_on, predicates, options } => {
                two!(input_left, input_right, |input_left, input_right| Join {
                    input_left,
                    input_right,
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                    predicates: predicates.clone(),
                    options: options.clone(),
                })
            },
            Gather { input, idxs, null_on_oob } => {
                two!(input, idxs, |input, idxs| Gather { input, idxs, null_on_oob: *null_on_oob })
            },
            HStack { input, exprs, options } => {
                one!(input, |input| HStack { input, exprs: exprs.clone(), options: options.clone() })
            },
            MatchToSchema { input, match_schema, per_column, extra_columns } => one!(input, |input| MatchToSchema {
                input,
                match_schema: match_schema.clone(),
                per_column: per_column.clone(),
                extra_columns: *extra_columns,
            }),
            PipeWithSchema { input, callback } => self
                .vec(input)?
                .map(|inputs| PipeWithSchema { input: Arc::from(inputs), callback: callback.clone() }),
            Pivot { input, on, on_columns, index, values, agg, maintain_order, separator, column_naming } => {
                one!(input, |input| Pivot {
                    input,
                    on: on.clone(),
                    on_columns: on_columns.clone(),
                    index: index.clone(),
                    values: values.clone(),
                    agg: agg.clone(),
                    maintain_order: *maintain_order,
                    separator: separator.clone(),
                    column_naming: *column_naming,
                })
            },
            Distinct { input, options } => one!(input, |input| Distinct { input, options: options.clone() }),
            Sort { input, by_column, slice, sort_options } => one!(input, |input| Sort {
                input,
                by_column: by_column.clone(),
                slice: *slice,
                sort_options: sort_options.clone(),
            }),
            Slice { input, offset, len } => one!(input, |input| Slice { input, offset: *offset, len: *len }),
            MapFunction { input, function } => one!(input, |input| MapFunction { input, function: function.clone() }),
            Union { inputs, args } => self.vec(inputs)?.map(|inputs| Union { inputs, args: args.clone() }),
            HConcat { inputs, options } => self.vec(inputs)?.map(|inputs| HConcat { inputs, options: options.clone() }),
            ExtContext { input, contexts } => {
                let new_input = self.arc(input)?;
                let new_contexts = self.vec(contexts)?;
                if new_input.is_none() && new_contexts.is_none() {
                    None
                } else {
                    Some(ExtContext {
                        input: new_input.unwrap_or_else(|| input.clone()),
                        contexts: new_contexts.unwrap_or_else(|| contexts.clone()),
                    })
                }
            },
            Sink { input, payload } => one!(input, |input| Sink { input, payload: payload.clone() }),
            SinkMultiple { inputs } => self.vec(inputs)?.map(|inputs| SinkMultiple { inputs }),
            MergeSorted { input_left, input_right, key, maintain_order } => {
                two!(input_left, input_right, |input_left, input_right| MergeSorted {
                    input_left,
                    input_right,
                    key: key.clone(),
                    maintain_order: *maintain_order,
                })
            },
            // A plan whose schema was read carries its converted IR; that IR still holds the anonymous scan, so drop
            // it and keep the rewritten DSL.
            IR { dsl, .. } => self.arc(dsl)?.map(|dsl| Arc::unwrap_or_clone(dsl)),
        })
    }
}
