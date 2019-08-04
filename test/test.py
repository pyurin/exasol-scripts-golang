import pyexasol
import pprint
import sys


host = sys.argv[1]

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=host, user='SYS', password='exasol')

# Create schema
stmt = C.execute("CREATE SCHEMA IF NOT EXISTS test")
C.open_schema("test")

# Set up go
stmt = C.execute("ALTER SESSION SET SCRIPT_LANGUAGES = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/go_entrypoint/go.sh'");


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




# Emit big data set
stmt = C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest(a DECIMAL(16,0), b DECIMAL(16,0)) EMITS (v DECIMAL(16,0), i DECIMAL(16,0), t VARCHAR(50)) AS

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
result = C.execute("SELECT test.gotest(1, 10000000)").fetchall()
if len(result) != 10 * 1000 * 1000:
    raise Exception("Emit big data set failed, incorrect len: \n", len(result))
i = 0
vSum = 0
for row in result:
    i = i + 1
    vSum += i
    expectedRow = (i, vSum, "string" + str(i))
    if row != expectedRow:
        raise Exception("Emit big data set failed, row \n", row, "but expected ", expectedRow)



# Read big data set
C.execute("""
CREATE OR REPLACE GO SET SCRIPT test.gotest(a DECIMAL(16,0), b DECIMAL(16,0)) RETURNS DECIMAL(16,0) AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) *int64 {
    var res int64;
    for true {
        if *iter.ReadInt64(0) > *iter.ReadInt64(1) {
            res++;
        }
        if !iter.Next() {
            break;
        }
    }
    return &res
}
/
""");

# sql emits 10M rows with incremental decimal starting from 0
result = C.execute("""
    WITH t AS
    (
            SELECT 0 as v UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
            SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    )
    SELECT
        test.gotest(v1, v2)
    FROM (
        SELECT
                7789312 v1,
                t7.v * 1000000 + t6.v * 100000 + t5.v * 10000 + t4.v * 1000 + t3.v * 100 + t2.v * 10 + t1.v v2
        FROM t t1
        CROSS JOIN t t2 CROSS JOIN t t3 CROSS JOIN t t4 CROSS JOIN t t5 CROSS JOIN t t6 CROSS JOIN t t7
    )
""").fetchall()
if result != [(7789312,)]:
    raise Exception("Read big data set failed, result set\n", result)



# Disconnect
C.close()