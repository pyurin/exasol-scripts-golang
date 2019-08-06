import pyexasol
import pprint
import sys
import _test_lib

C = _test_lib.init_exasol_test()

# Different data types types with column name access

stmt = C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest
          (a DECIMAL(36,0), b DECIMAL(36,36), c BOOLEAN, d DATE, e TIMESTAMP, f VARCHAR(2000000), g DOUBLE)
    EMITS
          (a DECIMAL(36,0), b DECIMAL(36,36), c BOOLEAN, d DATE, e TIMESTAMP, f VARCHAR(2000000), g DOUBLE)
AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) {

    if iter.ReadColIsNull("a") {
        iter.EmitNull()
    } else {
        iter.EmitIntBig(*iter.ReadColIntBig("a"))
    }

    if iter.ReadColIsNull("b") {
        iter.EmitNull()
    } else {
        iter.EmitDecimalApd(*iter.ReadColDecimalApd("b"))
    }

    if iter.ReadColIsNull("c") {
        iter.EmitNull()
    } else {
        iter.EmitBool(*iter.ReadColBool("c"))
    }

    if iter.ReadColIsNull("d") {
        iter.EmitNull()
    } else {
        iter.EmitTime(*iter.ReadColTime("d"))
    }

    if iter.ReadColIsNull("e") {
        iter.EmitNull()
    } else {
        iter.EmitTime(*iter.ReadColTime("e"))
    }

    if iter.ReadColIsNull("f") {
        iter.EmitNull()
    } else {
        iter.EmitString(*iter.ReadColString("f"))
    }

    if iter.ReadColIsNull("g") {
        iter.EmitNull()
    } else {
        iter.EmitFloat64(*iter.ReadColFloat64("g"))
    }

}
/
""");

# Fetch columns by names
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
    raise Exception("Fetch columns by names test failed, result set\n", result)
print("Fetch columns by names test - ok")


# Fetch columns by names (null values)
result = C.execute("SELECT test.gotest("
                   + ('9' * 36) + ", "
                   + '0.' + ('9' * 36) + ", "
                   + "NULL, "
                   + "'9999-12-31', "
                   + "'9999-12-31 23:59:59.999000', "
                   + "'" + ('ひ' * 100) + "',"
                   + "1.7e308"
                   + ")").fetchall()
if result != [('9' * 36, '0.' + ('9' * 36), None, "9999-12-31", "9999-12-31 23:59:59.999000", 'ひ' * 100, 1.7e308)]:
    raise Exception("Fetch columns by names (null values) test failed, result set\n", result)
print("Fetch columns by names (null values) test - ok")

