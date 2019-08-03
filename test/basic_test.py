import pyexasol
import pprint
import os

#host='localhost:8563'
host='192.168.1.172:8563'

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=host, user='SYS', password='exasol', schema='test')

# Create schema
stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")

# Set up go
stmt = C.execute("ALTER SYSTEM SET SCRIPT_LANGUAGES = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/go_entrypoint/go.sh'");

stmt = C.execute("""
CREATE OR REPLACE GO  SCALAR SCRIPT test.gotest(a DECIMAL(16,0), b DECIMAL(16,0)) EMITS (v DECIMAL(16,0), i DECIMAL(16,0)) AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) {
    var sumResult int64;
    for i := *iter.ReadInt64(0); i <= *iter.ReadInt64(1); i++ {
        sumResult += i;
        iter.EmitInt64(i)
        iter.EmitInt64(sumResult)
    }
}
/
""");

result = C.execute("SELECT test.gotest(1, 3)").fetchall()
if result != [(1, 1), (2, 3), (3, 6)]:
    raise Exception('Int64 series test failed')




# Disconnect
C.close()