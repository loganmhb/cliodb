from ctypes import *

cliodb = cdll.LoadLibrary("cliodb-ffi/target/debug/libcliodbffi.so")

cliodb.connect.argtypes = [c_char_p, c_void_p]
cliodb.connect.restype = c_int

cliodb.transact.argtypes = [c_void_p, c_char_p]
cliodb.transact.restype = c_int

# ValueTag enum
(VAL_ENTITY, VAL_IDENT, VAL_STRING, VAL_TIMESTAMP) = (0, 1, 2, 3)

class CValue(Structure):
    _fields_ = [
        ("tag", c_int64),
        ("string_val", c_char_p),
        ("int_val", c_int64)
    ]

    def value(self):
        if self.tag == VAL_ENTITY:
            return self.int_val
        elif self.tag == VAL_IDENT:
            # TODO: return an interned ident type
            return self.string_val.decode()
        elif self.tag == VAL_STRING:
            return self.string_val.decode()
        elif self.tag == VAL_TIMESTAMP:
            # TODO: return a real timestamp
            return self.string_val.decode()
        else:
            pass
          #  raise Exception("Unsupported tag: {}".format(self.tag))

ROW_CALLBACK = CFUNCTYPE(None, c_int32, POINTER(CValue))

def print_row(num_cols, c_val_p):
    row = []
    for i in range(num_cols):
        row.append(c_val_p[i].value())
    print(row)

cliodb.query.argtypes = [c_void_p, c_char_p, ROW_CALLBACK]

class Db(object):
    def __init__(self, db_ptr):
        self.db_ptr = db_ptr

    def __del__(self):
        cliodb.drop_db(self.db_ptr)

class ClioDB(object):
    def __init__(self, store_uri, tx_uri):
        """Takes a ClioDB URL and returns a connection."""
        if not store_uri:
            raise Exception("store_uri must be provided")
        if not tx_uri:
            raise Exception("tx_uri must be provided")
        self.conn_ptr = c_void_p()
        cliodb.connect(store_uri.encode('utf-8'), tx_uri.encode('utf-8'), byref(self.conn_ptr))

    def db(self):
        db_ptr = c_void_p()
        err = cliodb.get_db(self.conn_ptr, byref(db_ptr))
        if err < 0:
            # TODO: Set an error string
            raise Exception("Error opening db")
        return Db(db_ptr)

    def transact(self, tx_string):
        tx_bytes = tx_string.encode('utf-8')
        ret = cliodb.transact(self.conn_ptr, tx_bytes)
        if ret < 0:
            # TODO: Set an error string
            print("return value {}".format(ret))
            raise Exception("Error executing transaction")

    def close(self):
        cliodb.close(self.conn_ptr)


class Query(object):
    # TODO: queries should be parameterizable

    def __init__(self, query_string):
        self.query_string = query_string.encode('utf-8')

    def run(self, db):
        self.results = []
        def row_cb(num_cols, row_ptr):
            row = []
            for i in range(num_cols):
                row.append(row_ptr[i].value())
            self.results.append(row)

        cliodb.query(db.db_ptr, self.query_string, ROW_CALLBACK(row_cb))
        return self.results
