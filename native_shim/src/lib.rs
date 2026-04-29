use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[macro_use]
mod error;
mod utils;
mod types;
mod expr;
mod pl_io;
mod delta;
mod catalog;
mod eager;
mod lazy;
mod udf;
mod selectors;
mod sql;
mod series;
mod datatypes;
mod schema;
mod excel_reader;
mod excel_writer;
mod categories;
mod config;



