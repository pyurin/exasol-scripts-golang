import pyexasol
import pprint
import sys
import _test_lib

C = _test_lib.init_exasol_test()

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
print("Emit big data set - ok")
