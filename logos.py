from ctypes import *

logos = cdll.LoadLibrary("logos-ffi/target/debug/liblogosffi.so")

logos.connect.argtypes = [c_char_p, c_void_p]
logos.connect.restype = c_int

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

logos.query.argtypes = [c_void_p, c_char_p, ROW_CALLBACK]

uri = c_char_p(b"logos:mem://test")
conn = c_void_p()
print(logos.connect(uri, byref(conn)))

query = c_char_p(b"find ?e ?a where (?e ?a ?v)")

# print("python -- calling query")
# print(logos.query(conn, query))
# logos.query.argtypes = [c_void_p, c_char_p]
# logos.query.restype = POINTER(QueryResult)

# res = logos.query(conn, query).contents

class Query(object):
    def __init__(self, query_string):
        self.query_string = query_string

    def execute(self, conn):
        self.results = []
        def row_cb(num_cols, row_ptr):
            row = []
            for i in range(num_cols):
                row.append(row_ptr[i].value())
            self.results.append(row)

        logos.query(conn, self.query_string, ROW_CALLBACK(row_cb))
        return self.results
