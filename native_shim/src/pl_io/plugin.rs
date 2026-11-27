//! Managed readers as Polars I/O plugins (`polars.io_plugin.file_reader.v1`,
//! `polars.io_plugin.lazyframe_resolver.v1`).
//!
//! The contracts are implemented in .NET: it hands us the address of each contract struct (a table of its
//! `[UnmanagedCallersOnly]` entries, in native memory for the life of the process) under a package name. The engine
//! finds them through the registry's locator, by package and contract ID, wherever a query needs them; a LazyFrame
//! over a resolver handle is a `DslPlan::Resolver` node the optimizer resolves with the pushed projection, filters
//! and limit.
use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::{Arc, Mutex};

use polars::prelude::*;
use polars_io_ext_ffi::common::{
    ArrowArrayStream, ArrowSchema, FfiBuf, FfiError, FfiOption, FfiResult, FfiSlice, FfiStr, FfiVec,
};
use polars_io_ext_ffi::file_reader_v1::{self, BeginReadArgs, BuilderHandle, Capabilities};
use polars_io_ext_ffi::lazyframe_resolver_v1::{self, ResolveArgs, Resolved, ResolvedScan, ResolverHandle};
use polars_io_ext_ffi::registry;
use polars_plan::dsl::DslBuilder;
use polars_plan::dsl::dsl_resolver::DslResolver;
use polars_utils::pl_str::PlSmallStr;

use crate::types::LazyFrameContext;

/// (package, contract ID, address of the contract struct): registered from .NET, never removed.
static CONTRACTS: Mutex<Vec<(String, CString, usize)>> = Mutex::new(Vec::new());

fn locate(package: &str, id: &CStr) -> Result<*const c_void, String> {
    let contracts = CONTRACTS.lock().unwrap();
    match contracts.iter().find(|(p, i, _)| p == package && i.as_c_str() == id) {
        Some((_, _, ptr)) => Ok(*ptr as *const c_void),
        None => Err(format!(
            "the I/O plugin {package} does not provide {} in this process (it was never registered)",
            id.to_string_lossy()
        )),
    }
}

unsafe fn utf8<'a>(ptr: *const u8, len: usize, what: &str) -> PolarsResult<&'a str> {
    if len == 0 {
        return Ok("");
    }
    polars_ensure!(!ptr.is_null(), InvalidOperation: "{what} is null");
    std::str::from_utf8(unsafe { std::slice::from_raw_parts(ptr, len) })
        .map_err(|e| polars_err!(InvalidOperation: "{what} is not UTF-8: {e}"))
}

/// Registers the contract struct `contract` of the contract `id` under `package` (the struct must live for the rest
/// of the process). Installs the locator on the first call. Registering the same (package, id) again keeps the
/// first struct: the engine may hold its address already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_io_plugin_register(
    package: *const u8,
    package_len: usize,
    id: *const u8,
    id_len: usize,
    contract: *const c_void,
) {
    ffi_try_void!({
        let package = unsafe { utf8(package, package_len, "the plugin package") }?;
        let id = unsafe { utf8(id, id_len, "the contract id") }?;
        polars_ensure!(!package.is_empty(), InvalidOperation: "the plugin package is empty");
        polars_ensure!(!contract.is_null(), InvalidOperation: "the contract struct of {package} {id} is null");
        polars_ensure!(
            id == file_reader_v1::ID.to_str().unwrap() || id == lazyframe_resolver_v1::ID.to_str().unwrap(),
            InvalidOperation: "unknown I/O plugin contract {id}"
        );
        let id = CString::new(id).map_err(|e| polars_err!(InvalidOperation: "{e}"))?;
        {
            let mut contracts = CONTRACTS.lock().unwrap();
            if !contracts.iter().any(|(p, i, _)| p == package && *i == id) {
                contracts.push((package.to_owned(), id, contract as usize));
            }
        }
        // Idempotent: a locator installed already is kept (it is this one; nothing else installs one here).
        registry::set_locator(Box::new(locate));
        Ok(())
    })
}

/// A LazyFrame over the resolver `handle` of the plugin `package`; takes over one reference to the handle (released
/// also when this fails).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_scan_io_plugin(
    package: *const u8,
    package_len: usize,
    handle: *const c_void,
) -> *mut LazyFrameContext {
    ffi_try!({
        let package = unsafe { utf8(package, package_len, "the plugin package") }?;
        let handle = ResolverHandle(handle);
        let plugin = match polars_io::io_plugin_ffi::lazyframe_resolver_v1(package) {
            Ok(plugin) => plugin,
            // Without the contract there is no release function: the reference leaks (as the engine's own code).
            Err(e) => return Err(e),
        };
        // Owns the reference from here on: released when the node is dropped (or right away on an error).
        let resolver = unsafe { DslResolver::new_ffi(PlSmallStr::from(package), handle) }.inspect_err(|_| unsafe {
            (plugin.resolver_release)(handle)
        })?;
        let lf = LazyFrame::from(DslBuilder::from_dsl_resolver(Arc::new(resolver)).build());
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

// ------------------------------------------------------------------------------------------- layout

fn ok_offset<T>(r: &FfiResult<T>) -> usize {
    match r {
        FfiResult::Ok(v) => v as *const T as usize - r as *const FfiResult<T> as usize,
        FfiResult::Err(_) => unreachable!(),
    }
}

fn err_offset<T>(r: &FfiResult<T>) -> usize {
    match r {
        FfiResult::Err(e) => e as *const FfiError as usize - r as *const FfiResult<T> as usize,
        FfiResult::Ok(_) => unreachable!(),
    }
}

fn some_offset<T>(o: &FfiOption<T>) -> usize {
    match o {
        FfiOption::Some(v) => v as *const T as usize - o as *const FfiOption<T> as usize,
        FfiOption::None => unreachable!(),
    }
}

fn tag<T>(v: &T) -> u8 {
    // SAFETY: the tag of a `repr(C, u8)` enum is its first byte.
    unsafe { *(v as *const T as *const u8) }
}

fn empty_stream() -> ArrowArrayStream {
    ArrowArrayStream { get_schema: None, get_next: None, get_last_error: None, release: None, private_data: std::ptr::null_mut() }
}

fn empty_capabilities() -> Capabilities {
    Capabilities { flags: 0, max_concurrent_scans: FfiOption::None, preferred_remote_pre_init: FfiOption::None }
}

/// The sizes, alignments and offsets of the contract types, as `name=value` lines, for the .NET mirrors to check
/// (a `repr(C, u8)` enum has no C declaration to read them from). Free with `pl_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn pl_io_plugin_layout() -> *mut c_char {
    let mut lines: Vec<String> = Vec::new();
    let mut put = |name: &str, value: usize| lines.push(format!("{name}={value}"));
    macro_rules! size {
        ($name:literal, $t:ty) => {
            put(concat!($name, ".size"), std::mem::size_of::<$t>());
            put(concat!($name, ".align"), std::mem::align_of::<$t>());
        };
    }
    macro_rules! field {
        ($name:literal, $t:ty, $f:ident) => {
            put(concat!($name, ".", stringify!($f)), std::mem::offset_of!($t, $f));
        };
    }
    macro_rules! result {
        ($name:literal, $t:ty, $ok:expr) => {
            size!($name, FfiResult<$t>);
            let ok: FfiResult<$t> = FfiResult::Ok($ok);
            put(concat!($name, ".ok"), ok_offset(&ok));
            put(concat!($name, ".tag_ok"), tag(&ok) as usize);
            let err: FfiResult<$t> = FfiResult::Err(FfiError { kind: 0, msg: FfiBuf::empty() });
            put(concat!($name, ".err"), err_offset(&err));
            put(concat!($name, ".tag_err"), tag(&err) as usize);
        };
    }
    macro_rules! option {
        ($name:literal, $t:ty, $some:expr) => {
            size!($name, FfiOption<$t>);
            let some: FfiOption<$t> = FfiOption::Some($some);
            put(concat!($name, ".some"), some_offset(&some));
            put(concat!($name, ".tag_some"), tag(&some) as usize);
            let none: FfiOption<$t> = FfiOption::None;
            put(concat!($name, ".tag_none"), tag(&none) as usize);
        };
    }

    size!("FfiStr", FfiStr<'static>);
    size!("FfiBuf", FfiBuf);
    size!("FfiSlice", FfiSlice<'static, u8>);
    size!("FfiVec", FfiVec<FfiBuf>);
    size!("FfiError", FfiError);
    field!("FfiError", FfiError, kind);
    field!("FfiError", FfiError, msg);
    size!("ArrowSchema", ArrowSchema);
    size!("ArrowArrayStream", ArrowArrayStream);

    option!("OptionU64", u64, 0);
    option!("OptionStr", FfiStr<'static>, FfiStr::new(""));
    option!("OptionStrSlice", FfiSlice<'static, FfiStr<'static>>, FfiSlice::new(&[]));
    option!("OptionBuf", FfiBuf, FfiBuf::empty());

    result!("ResultHandle", BuilderHandle, BuilderHandle(std::ptr::null()));
    result!("ResultBuf", FfiBuf, FfiBuf::empty());
    result!("ResultU64", u64, 0);
    result!("ResultUnit", (), ());
    result!("ResultCapabilities", Capabilities, empty_capabilities());
    result!("ResultSchema", ArrowSchema, ArrowSchema::empty());
    result!("ResultStream", ArrowArrayStream, empty_stream());
    result!("ResultBufVec", FfiVec<FfiBuf>, FfiVec::from_vec(Vec::new()));
    result!("ResultResolved", Resolved, Resolved { scan: FfiOption::None, version_key: FfiOption::None });

    size!("Capabilities", Capabilities);
    field!("Capabilities", Capabilities, flags);
    field!("Capabilities", Capabilities, max_concurrent_scans);
    field!("Capabilities", Capabilities, preferred_remote_pre_init);

    size!("BeginReadArgs", BeginReadArgs<'static>);
    field!("BeginReadArgs", BeginReadArgs<'static>, columns);
    field!("BeginReadArgs", BeginReadArgs<'static>, verbose);

    size!("ResolveArgs", ResolveArgs<'static>);
    field!("ResolveArgs", ResolveArgs<'static>, projection);
    field!("ResolveArgs", ResolveArgs<'static>, limit);
    field!("ResolveArgs", ResolveArgs<'static>, filters);
    field!("ResolveArgs", ResolveArgs<'static>, filter_columns);
    field!("ResolveArgs", ResolveArgs<'static>, filter_drop_columns_idx);
    field!("ResolveArgs", ResolveArgs<'static>, existing_version_key);
    field!("ResolveArgs", ResolveArgs<'static>, target_n_sources);
    field!("ResolveArgs", ResolveArgs<'static>, verbose);

    size!("ResolvedScan", ResolvedScan);
    field!("ResolvedScan", ResolvedScan, schema);
    field!("ResolvedScan", ResolvedScan, sources);
    field!("ResolvedScan", ResolvedScan, reader_package);
    field!("ResolvedScan", ResolvedScan, reader);
    field!("ResolvedScan", ResolvedScan, applied_filters);
    field!("ResolvedScan", ResolvedScan, row_estimate);

    size!("Resolved", Resolved);
    field!("Resolved", Resolved, scan);
    field!("Resolved", Resolved, version_key);
    let scan = FfiOption::Some(ResolvedScan {
        schema: ArrowSchema::empty(),
        sources: FfiVec::from_vec(Vec::new()),
        reader_package: FfiBuf::empty(),
        reader: BuilderHandle(std::ptr::null()),
        applied_filters: FfiVec::from_vec(Vec::new()),
        row_estimate: FfiOption::None,
    });
    put("OptionResolvedScan.size", std::mem::size_of::<FfiOption<ResolvedScan>>());
    put("OptionResolvedScan.some", some_offset(&scan));
    drop(scan);

    size!("FileReaderPlugin", file_reader_v1::Plugin);
    size!("ResolverPlugin", lazyframe_resolver_v1::Plugin);

    let text = lines.join("\n");
    CString::new(text).unwrap().into_raw()
}
