import pyexasol
import pprint
import sys
import _test_lib

C = _test_lib.init_exasol_test()

# SET SCRIPT RETURNS, SCALAR SCRIPT EMITS are tested in other tests.
# Let's also double check that SCALAR SCRIPT RETURNS and SET SCRIPT EMITS work

# SCALAR SCRIPT RETURNS

stmt = C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest
          (a DECIMAL(16,0))
    RETURNS
          DECIMAL(16,0)
AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) *int64 {
    return iter.ReadInt64(0)
}
/
""");

result = C.execute("SELECT test.gotest(v) FROM (SELECT 16 as v UNION ALL SELECT 17 UNION ALL SELECT 21)").fetchall()
if result != [(16,),(17,),(21,)]:
    raise Exception("'Scalar script returns' test failed, result set\n", result)
print("'Scalar script returns' - ok")

# SET SCRIPT EMITS

stmt = C.execute("""
CREATE OR REPLACE GO SET SCRIPT test.gotest
          (a DECIMAL(16,0), b BOOLEAN)
    EMITS
          (a DECIMAL(16,0), b VARCHAR(50))
AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) {
    for true {
        if *iter.ReadBool(1) == true {
            var i int64;
            for i = *iter.ReadInt64(0); i > 2; i = i / 2 {
                iter.EmitInt64(i)
                iter.EmitString("Cycle in progress")
            }
            iter.EmitInt64(i)
            iter.EmitString("Cycle finished")
        }
        if !iter.Next() {
            break;
        }
    }
}
/
""");

result = C.execute("SELECT test.gotest(a, b) FROM ("
                   "SELECT 17 as a, false as b "
                   "UNION ALL "
                   "SELECT 63 as a, false as b "
                   "UNION ALL "
                   "SELECT 17 as a, false as b "
                   "UNION ALL "
                   "SELECT 4 as a, true as b "
                   "UNION ALL "
                   "SELECT 16 as a, true as b "
                   "UNION ALL "
                   "SELECT 96 as a, false as b "
                   ")").fetchall()
if result != [(4,"Cycle in progress"),(2,"Cycle finished"),(16,"Cycle in progress"),(8,"Cycle in progress"),(4,"Cycle in progress"),(2,"Cycle finished")]:
    raise Exception("'Set script emits' test failed, result set\n", result)
print("'Set script emits' - ok")