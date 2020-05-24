extern crate cliodb;
extern crate zmq;

use std::ffi::{CStr, CString};
use std::mem;
use std::os::raw::{c_char, c_int, c_long};

use cliodb::{Result, Value, Relation, TxReport};
use cliodb::conn::{Conn, store_from_uri};
use cliodb::db::Db;

fn conn_from_c_strings(store_uri: &CStr, tx_addr: &CStr) -> Result<Conn> {
    let store = store_from_uri(store_uri.to_str()?)?;
    let context = zmq::Context::new();
    let conn = Conn::new(store, tx_addr.to_str()?, &context)?;
    Ok(conn)
}

#[no_mangle]
pub extern "C" fn connect(uri_ptr: *mut c_char, tx_ptr: *mut c_char, ret_ptr: *mut *mut Conn) -> c_int {
    let uri = unsafe { CStr::from_ptr(uri_ptr) };
    let tx_addr = unsafe { CStr::from_ptr(tx_ptr) };
    match conn_from_c_strings(uri, tx_addr) {
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
    Boolean = 4,
    Long = 5,
}

impl<'a> From<&'a Value> for CValue {
    fn from(v: &Value) -> CValue {
        match *v {
            Value::String(ref s) => CValue::string(s),
            Value::Ref(cliodb::Entity(e)) => CValue::entity(e),
            Value::Ident(ref i) => CValue::string(i),
            Value::Timestamp(t) => CValue::string(&t.to_string()),
            Value::Boolean(b) => CValue::boolean(b),
            Value::Long(l) => CValue::long(l),
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

    fn boolean(val: bool) -> CValue {
        CValue {
            tag: ValueTag::Boolean,
            string_val: CString::default().into_raw(),
            int_val: if val { 1 } else { 0 },
        }
    }

    fn long(val: i64) -> CValue {
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
    query_string_ptr: *const c_char,
    cb: extern "C" fn(num_items: c_int, row: *const CValue),
) -> c_int {
    let db: &Db = unsafe { &*db_ptr };
    let query_str = unsafe { CStr::from_ptr(query_string_ptr) };
    let q = match cliodb::parse_query(query_str.to_str().unwrap()) {
        Ok(q) => q,
        Err(err) => {
            // FIXME: implement a more robust way to retrieve error msgs
            println!("error {}", err);
            return -1;
        }
    };

    match cliodb::query(q, &db) {
        Ok(Relation(vars, rows)) => {
            for row in rows {
                let row_vec: Vec<CValue> = row.iter().map(|v| v.into()).collect();
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
    let tx = match cliodb::parse_tx(tx_str.to_str().unwrap()) {
        Ok(tx) => tx,
        // FIXME: signal error
        Err(e) => {
            println!("error {:?}", e);
            return -1;
        }
    };

    match conn.transact(tx) {
        // FIXME: Return list of new entities
        // (via result callback like query?)
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
