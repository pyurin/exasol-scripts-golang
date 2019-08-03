import pyexasol
import pprint
import os

host='localhost:8563'
#host='192.168.1.172:8563'

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=host, user='SYS', password='exasol', schema='test')

# Create schema
stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")

# Set up go
stmt = C.execute("ALTER SYSTEM SET SCRIPT_LANGUAGES = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/go_entrypoint/go.sh'");




# Basic dataset emit test

stmt = C.execute("""
CREATE OR REPLACE GO  SCALAR SCRIPT test.gotest(a DECIMAL(16,0), b DECIMAL(16,0)) EMITS (v DECIMAL(16,0), i DECIMAL(16,0), t VARCHAR(10)) AS

package main

import \"exago\"
import \"fmt\"

func Run(iter *exago.ExaIter) {
    var sumResult int64;
    for i := *iter.ReadInt64(0); i <= *iter.ReadInt64(1); i++ {
        sumResult += i;
        iter.EmitInt64(i)
        iter.EmitInt64(sumResult)
        iter.EmitString(fmt.Sprint("string", i))
    }
}
/
""");

result = C.execute("SELECT test.gotest(1, 4)").fetchall()
if result != [(1, 1, "string1"), (2, 3, "string2"), (3, 6, "string3"), (4, 10, "string4")]:
    raise Exception("Basic test 1 failed, result set\n", result)


# Different data types test

stmt = C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest(a DECIMAL(36,0), b DECIMAL(36,36))
    EMITS (a DECIMAL(36,0), b DECIMAL(36,36)) AS

package main

import \"exago\"
import \"github.com/cockroachdb/apd\"
import \"math/big\"

func Run(iter *exago.ExaIter) {
    bigInt := iter.ReadIntBig(0)
    bigInt2 := big.NewInt(int64(2))
    bigInt.Mul(bigInt, bigInt2)
    iter.EmitIntBig(*bigInt)

    dec := *iter.ReadDecimalApd(1)
    c:= apd.BaseContext.WithPrecision(36)
    c.Quo(dec, dec, apd.New(2, 0))
    iter.EmitDecimalApd(dec)
}
/
""");

result = C.execute("SELECT test.gotest(1, 0.4)").fetchall()
if result != [(1, 1)]:
    raise Exception("Different data types test failed, result set\n", result)


# Disconnect
C.close()