from ctypes import *

logos = cdll.LoadLibrary("logos-ffi/target/debug/liblogosffi.so")

logos.connect.argtypes = [c_char_p, c_void_p]
logos.connect.restype = c_int

uri = c_char_p(b"logos:mem://test")
conn = c_void_p()
print(logos.connect(uri, byref(conn)))

query = c_char_p(b"find ?e ?a ?v where (?e ?a ?v)")

print(conn)
print(query)
logos.query.argtypes = [c_void_p, c_char_p]
logos.query.restype = c_char_p

print("python -- calling query")
print(logos.query(conn, query))
