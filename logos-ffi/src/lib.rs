extern crate logos;

use std::ffi::{CStr, OsString, CString};
use std::mem;
use std::os::raw::{c_void, c_char, c_int};

use logos::{Result, Value};
use logos::parse_query;
use logos::db::{Conn, store_from_uri};

fn conn_from_c_string(uri: &CStr) -> Result<Conn> {
    let uri = uri.to_str()?;
    let store = store_from_uri(uri)?;
    let conn = Conn::new(store)?;
    Ok(conn)
}

#[no_mangle]
pub extern "C" fn connect(uri_ptr: *mut c_char, ret_ptr: *mut *mut Conn) -> c_int {
    let uri = unsafe { CStr::from_ptr(uri_ptr) };
    match conn_from_c_string(uri) {
        Ok(conn) => {
            unsafe { *ret_ptr = mem::transmute(Box::new(conn)) };
            return 0;
        }
        Err(e) => {
            println!("{:?}", e);
            return -1;
        }
    }
}

/// The result of a query is an array of arrays, where the nested
/// arrays are the result tuples. This struct encodes the necessary
/// information to pass the nested array across FFI boundaries.  This
/// is tricky because queries return tuples of values, which are enums
/// and therefore can't be passed across FFI boundaries. Instead we
/// have to make a C-style tagged union, and include a key indicated
/// which types the tuple elements should be cast to.
#[repr(C)]
pub struct QueryResult {
    /// The number of tuples returned by the query.
    num_results: usize,
    /// The width of each tuple, i.e. how many variables the query bound.
    tuple_width: usize,
    /// They key; an array containing a tag for each tuple position.
    tuple_types: *const c_void,
    /// The results themselves, an array of arrays.
    results: *const *const c_void,
    /// A field indicating an error. Null when no error.
    error: *const c_char,
}

#[repr(C)]
pub enum ValueTag {
    Entity,
    Ident,
    String,
    Timestamp,
}

fn value_tag(v: Value) -> ValueTag {
    match v {
        Value::String(_) => ValueTag::String,
        Value::Entity(_) => ValueTag::Entity,
        Value::Ident(_) => ValueTag::Ident,
        Value::Timestamp(_) => ValueTag::Timestamp,
    }
}

unsafe fn format_query_result(result: logos::QueryResult) -> *const QueryResult {
    let logos::QueryResult(vars, maps) = result;
    let tuple_width = vars.len();
    let num_results = maps.len();

    if num_results == 0 {
        mem::transmute(Box::new(QueryResult {
                                    num_results: 0,
                                    tuple_width: 0,
                                    tuple_types: std::ptr::null(),
                                    results: std::ptr::null(),
                                    error: std::ptr::null(),
                                }))
    } else {
        let mut results = vec![];
        // Set up the tuple key.
        let tuple_types: Vec<ValueTag> = vars.iter()
            .map(|v| value_tag(maps[0][v].clone()))
            .collect();

        for m in maps {
            let mut tuple = vec![];
            for var in vars.iter() {
                tuple.push(m[&var].clone());
            }
            let tuple_ptr = tuple.as_ptr();
            mem::forget(tuple_ptr);
            results.push(tuple_ptr);
        }

        let tuple_types_ptr = tuple_types.as_ptr() as *const _ as *const c_void;
        let results_ptr = results.as_ptr() as *const _ as *const *const c_void;

        mem::forget(tuple_types);
        mem::forget(results);

        mem::transmute(Box::new(QueryResult {
                                    num_results,
                                    tuple_width,
                                    tuple_types: tuple_types_ptr,
                                    results: results_ptr,
                                    error: std::ptr::null(),
                                }))
    }
}

unsafe fn format_error_result(e: logos::Error) -> *const QueryResult {
    mem::transmute(Box::new(QueryResult {
                                num_results: 0,
                                tuple_width: 0,
                                tuple_types: std::ptr::null(),
                                results: std::ptr::null(),
                                error: CString::new(format!("{:?}", e)).unwrap().as_ptr(),
                            }))
}

#[no_mangle]
pub extern "C" fn query(ptr: *mut Conn, query: *const c_char) -> *const QueryResult {
    let conn: &Conn = unsafe { &*ptr };
    let query_str = unsafe { CStr::from_ptr(query) };
    let db = match conn.db() {
        Ok(db) => db,
        Err(e) => return unsafe {format_error_result(e)},
    };
    let q = parse_query(query_str.to_str().unwrap()).unwrap();

    match db.query(&q) {
        Ok(res) => unsafe { format_query_result(res) },
        Err(e) => unsafe { format_error_result(e) }
    }
}

// FIXME: need a way to destroy a conn!
