import pyexasol
import pprint
import sys


def init_exasol_test():
    host = sys.argv[1]

    printer = pprint.PrettyPrinter(indent=4, width=140)

    # Basic connect
    C = pyexasol.connect(dsn=host, user='SYS', password='exasol')

    # Create schema
    stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")
    C.open_schema("test")

    # Set up go
    stmt = C.execute("ALTER SESSION SET SCRIPT_LANGUAGES = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/src/exago.sh'");

    return C
