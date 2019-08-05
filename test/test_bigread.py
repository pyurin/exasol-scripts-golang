import pyexasol
import pprint
import sys
import _test_lib

C = _test_lib.init_exasol_test()

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

# sql creates 10M rows with incremental decimal starting from 0 and passes to script
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
print("Read big data set - ok")
