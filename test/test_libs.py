import pyexasol
import pprint
import sys
import os
import _test_lib

C = _test_lib.init_exasol_test()

if len(sys.argv) < 4:
    raise Exception("Usage: " + sys.argv[0] + " EXASOL_DB_HOST_PORT EXASOL_BUCKET_HOST_PORT EXASOL_BUCKETFS_USERPASS")

bucketHostPort = sys.argv[2]
bucketUserPass = sys.argv[3]

# Using local lib ./test/testnumbers
r = os.system(os.path.dirname(os.path.realpath(__file__)) + "/../upload_lib_local.sh " + bucketHostPort + " 'default/go' " + "'" + bucketUserPass + "' " + os.path.dirname(os.path.realpath(__file__)) + "/testnumbers")
if r != 0:
    raise Exception("Failed running upload_lib_local.sh")

C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest(a DECIMAL(16,0)) RETURNS BOOLEAN AS

package main

import "exago"
import "testnumbers"

func Run(iter *exago.ExaIter) *bool {
    b := testnumbers.IsOdd(*iter.ReadInt64(0))
    return &b;
}
""")
result = C.execute("SELECT test.gotest(2015)").fetchall()
if result != [(True,)]:
    raise Exception("Local lib test failed, result set\n", result)
print("Local lib test - ok")



# Using github lib https://github.com/visualfc/fibutil
r = os.system(os.path.dirname(os.path.realpath(__file__)) + "/../upload_lib_github.sh " + bucketHostPort + " 'default/go' " + "'" + bucketUserPass + "' " + "https://github.com/visualfc/fibutil")
if r != 0:
    raise Exception("Failed running upload_lib_github.sh")

C.execute("""
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest(a DECIMAL(16,0)) RETURNS DECIMAL(36, 0) AS

package main

import "exago"
import "math/big"
import "github.com/visualfc/fibutil/fib"

func Run(iter *exago.ExaIter) *big.Int {
    return fib.Fib(*iter.ReadInt64(0));
}
""")
result = C.execute("SELECT test.gotest(10)").fetchall()
if result != [('55',)]:
    raise Exception("Github lib test failed, result set\n", result)
print("Github lib test - ok")