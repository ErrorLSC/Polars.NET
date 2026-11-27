//! The Map namespace (Polars' `Expr::map_()`; `map` is the elementwise UDF entry point) and `list.to_map`.
use polars::prelude::*;
use crate::{gen_namespace_unary, impl_expr_namespace_expr_arg};
use crate::types::ExprContext;

gen_namespace_unary!(pl_expr_map_entries, map_, entries);
gen_namespace_unary!(pl_expr_map_keys, map_, keys);
gen_namespace_unary!(pl_expr_map_values, map_, values);
gen_namespace_unary!(pl_expr_map_len, map_, len);

impl_expr_namespace_expr_arg!(pl_expr_map_contains_key, map_, contains_key);
impl_expr_namespace_expr_arg!(pl_expr_map_get, map_, get);

gen_namespace_unary!(pl_expr_list_to_map, list, to_map);
