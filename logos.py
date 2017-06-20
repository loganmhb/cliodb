from ctypes import *

logos = cdll.LoadLibrary("logos-ffi/target/debug/liblogosffi.so")

logos.connect.argtypes = [c_char_p, c_void_p]
logos.connect.restype = c_int

uri = c_char_p(b"logos:mem://test")
conn = c_void_p()
print(logos.connect(uri, byref(conn)))

query = c_char_p(b"find ?e ?a ?v where (?e ?a ?v)")

print("python -- calling query")
print(logos.query(conn, query))

# ValueType union
(VAL_ENTITY, VAL_IDENT, VAL_STRING, VAL_TIMESTAMP) = (0, 1, 2, 3)

class QueryResult(Structure):
    _fields_ = (
        ("num_results", c_size_t),
        ("tuple_width", c_size_t),
        ("tuple_types", POINTER(c_ubyte)),
        ("results", POINTER(c_void_p)),
        ("error", c_char_p)
    )

class LogosValue(Union):
    _fields_ = (
        ("string", c_char_p),
        ("ident", c_char_p),
        ("timestamp", c_char_p),
        ("entity", c_size_t)
    )

logos.query.argtypes = [c_void_p, c_char_p]
logos.query.restype = POINTER(QueryResult)

res = logos.query(conn, query).contents
