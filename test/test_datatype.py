import pyexasol
import pprint
import sys
import _test_lib

C = _test_lib.init_exasol_test()

# Just different data types test

stmt = C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest
          (a DECIMAL(36,0), b DECIMAL(36,36), c BOOLEAN, d DATE, e TIMESTAMP, f VARCHAR(2000000), g DOUBLE)
    EMITS
          (a DECIMAL(36,0), b DECIMAL(36,36), c BOOLEAN, d DATE, e TIMESTAMP, f VARCHAR(2000000), g DOUBLE)
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

    if iter.ReadIsNull(6) {
        iter.EmitNull()
    } else {
        iter.EmitFloat64(*iter.ReadFloat64(6))
    }

}
/
""");

# Big values
result = C.execute("SELECT test.gotest("
                   + ('9' * 36) + ", "
                   + '0.' + ('9' * 36) + ", "
                   + "True, "
                   + "'9999-12-31', "
                   + "'9999-12-31 23:59:59.999000', "
                   + "'" + ('ひ' * 100) + "',"
                   + "1.7e308"
                   + ")").fetchall()
if result != [('9' * 36, '0.' + ('9' * 36), True, "9999-12-31", "9999-12-31 23:59:59.999000", 'ひ' * 100, 1.7e308)]:
    raise Exception("Big values test failed, result set\n", result)
print("Big values test - ok")


# Small data types
result = C.execute("SELECT test.gotest("
                   + "-" + ('9' * 36) + ", "
                   + '-0.' + ('9' * 36) + ", "
                   + "False, "
                   + "'0001-01-01', "
                   + "'0001-01-01 00:00:00.000000', "
                   + "'',"
                   + "-1.7e308"
                   + ")").fetchall()
if result != [('-' + '9' * 36, '-0.' + ('9' * 36), False, "0001-01-01", "0001-01-01 00:00:00.000000", None, -1.7e308)]:
    raise Exception("Small values test failed, result set\n", result)
print("Small values test - ok")


# Null data type
result = C.execute("SELECT test.gotest("
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL"
                   + ")").fetchall()
if result != [(None, None, None, None, None, None, None)]:
    raise Exception("Null values test failed, result set\n", result)
print("Null values test - ok")



# Big string test
result = C.execute("SELECT test.gotest("
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + " NULL , "
                   + "'" + ('ひ' * 2000000) + "',"
                   + " NULL"
                   + ")").fetchall()
if result != [(None, None, None, None, None, ('ひ' * 2000000), None)]:
    raise Exception("Big string test failed, result set (could be huge!)\n", result)
print("Big string test - ok")
