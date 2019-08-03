import pyexasol
import pprint
import os
import decimal
import time

host='localhost:8899'
#host='192.168.1.172:8563'

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=host, user='SYS', password='exasol')

# Create schema
stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")
C.open_schema("test")

# Set up go
stmt = C.execute("ALTER SESSION SET SCRIPT_LANGUAGES = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/go_entrypoint/go.sh'");



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
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest
          (a DECIMAL(36,0), b DECIMAL(36,36), c BOOLEAN, d DATE, e TIMESTAMP, f VARCHAR(100))
    EMITS
          (a DECIMAL(36,0), b DECIMAL(36,36), c BOOLEAN, d DATE, e TIMESTAMP, f VARCHAR(100))
AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) {

    if iter.ReadIsNull(0) {
        iter.EmitNull()
    } else {
        iter.EmitIntBig(*iter.ReadIntBig(0))
    }

    if iter.ReadIsNull(1) {
        iter.EmitNull()
    } else {
        iter.EmitDecimalApd(*iter.ReadDecimalApd(1))
    }

    if iter.ReadIsNull(2) {
        iter.EmitNull()
    } else {
        iter.EmitBool(*iter.ReadBool(2))
    }

    if iter.ReadIsNull(3) {
        iter.EmitNull()
    } else {
        iter.EmitTime(*iter.ReadTime(3))
    }

    if iter.ReadIsNull(4) {
        iter.EmitNull()
    } else {
        iter.EmitTime(*iter.ReadTime(4))
    }

    if iter.ReadIsNull(5) {
        iter.EmitNull()
    } else {
        iter.EmitString(*iter.ReadString(5))
    }

}
/
""");

# Big data types
result = C.execute("SELECT test.gotest("
                   + ('9' * 36) + ", "
                   + '0.' + ('9' * 36) + ", "
                   + "True, "
                   + "'9999-12-31', "
                   + "'9999-12-31 23:59:59.999000', "
                   + "'" + ('ひ' * 100) + "'"
                   + ")").fetchall()
if result != [('9' * 36, '0.' + ('9' * 36), True, "9999-12-31", "9999-12-31 23:59:59.999000", 'ひ' * 100)]:
    raise Exception("Big data types test failed, result set\n", result)


# Small data types
result = C.execute("SELECT test.gotest("
                   + "-" + ('9' * 36) + ", "
                   + '-0.' + ('9' * 36) + ", "
                   + "False, "
                   + "'0001-01-01', "
                   + "'0001-01-01 00:00:00.000000', "
                   + "''"
                   + ")").fetchall()
if result != [('-' + '9' * 36, '-0.' + ('9' * 36), False, "0001-01-01", "0001-01-01 00:00:00.000000", None)]:
    raise Exception("Small data types test failed, result set\n", result)


# Null data type
result = C.execute("SELECT test.gotest("
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL"
                   + ")").fetchall()
if result != [(None, None, None, None, None, None)]:
    raise Exception("Null data type test failed, result set\n", result)


# Disconnect
C.close()