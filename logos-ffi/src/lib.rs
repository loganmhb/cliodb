extern crate logos;

use std::ffi::{CStr, CString};
use std::mem;
use std::os::raw::{c_char, c_int, c_long};

use logos::{Result, Value, Relation, TxReport};
use logos::{parse_query, parse_tx};
use logos::conn::{Conn, store_from_uri};
use logos::db::Db;

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

#[no_mangle]
pub extern "C" fn get_db(conn_ptr: *mut Conn, ret_ptr: *mut *mut Db) -> c_int {
    let db_result = unsafe { (*conn_ptr).db() };
    match db_result {
        Ok(db) => {
            unsafe { *ret_ptr = mem::transmute(Box::new(db)) };
            return 0;
        }
        Err(e) => {
            println!("{:?}", e);
            return -1;
        }
    }
}

#[no_mangle]
pub extern "C" fn drop_db(db: *mut Db) -> c_int {
    unsafe {
        let _ = Box::from_raw(db);
    }

    0
}

#[no_mangle]
/// Drops the connection created by `connect`.
pub extern "C" fn close(conn: *mut Conn) {
    unsafe {
        let _ = Box::from_raw(conn);
    }
}

#[repr(u64)]
#[derive(Debug, Clone)]
pub enum ValueTag {
    Entity = 0,
    Ident = 1,
    String = 2,
    Timestamp = 3,
}

impl<'a> From<&'a Value> for CValue {
    fn from(v: &Value) -> CValue {
        match *v {
            Value::String(ref s) => CValue::string(s),
            Value::Entity(logos::Entity(e)) => CValue::entity(e),
            Value::Ident(ref i) => CValue::string(i),
            Value::Timestamp(t) => CValue::string(&t.to_string()),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct CValue {
    tag: ValueTag,
    string_val: *const c_char,
    int_val: c_long,
}

// These functions are leaky to facilitate passing the resulting
// structs over the FFI.  You MUST reclaim the string_val field with
// `CString::from_raw` in order to avoid a memory leak.
impl CValue {
    fn string(val: &str) -> CValue {
        CValue {
            tag: ValueTag::String,
            string_val: CString::new(val).unwrap().into_raw(),
            int_val: 0,
        }
    }

    fn entity(val: i64) -> CValue {
        CValue {
            tag: ValueTag::Entity,
            string_val: CString::default().into_raw(),
            int_val: val as c_long,
        }
    }
}

#[no_mangle]
pub extern "C" fn query(
    db_ptr: *mut Db,
    query: *const c_char,
    cb: extern "C" fn(num_items: c_int, row: *const CValue),
) -> c_int {
    let db: &Db = unsafe { &*db_ptr };
    let query_str = unsafe { CStr::from_ptr(query) };
    let q = match parse_query(query_str.to_str().unwrap()) {
        Ok(query) => query,
        Err(err) => {
            // FIXME: implement a more robust way to retrieve error msgs
            println!("error {}", err);
            return -1;
        }
    };

    match db.query(&q) {
        Ok(Relation(vars, rows)) => {
            for row in rows {
                let row_vec: Vec<CValue> =
                    vars.iter().map(|k| row.get(k).unwrap().into()).collect();
                cb(vars.len() as i32, row_vec.as_ptr());
                // Free the CString's value.
                for val in row_vec {
                    unsafe {
                        CString::from_raw(val.string_val as *mut i8);
                    }
                }
            }
            return 0;
        }
        Err(e) => {
            println!("error {:?}", e);
            return -1;
        }
    }
}

#[no_mangle]
pub extern "C" fn transact(conn_ptr: *mut Conn, tx_ptr: *const c_char) -> c_int {
    let conn: &Conn = unsafe { &*conn_ptr };
    let tx_str = unsafe { CStr::from_ptr(tx_ptr) };
    let tx = match parse_tx(tx_str.to_str().unwrap()) {
        Ok(tx) => tx,
        // FIXME: signal error
        Err(e) => {
            println!("error {:?}", e);
            return -1;
        }
    };

    match conn.transact(tx) {
        // FIXME: Return list of new entities
        Ok(TxReport::Success { .. }) => return 0,
        // FIXME: Signal error
        Ok(TxReport::Failure(f)) => {
            println!("error {:?}", f);
            return -1;
        },
        Err(e) => {
            println!("error {:?}", e);
            return -1;
        },
    }
}
