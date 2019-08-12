[![Build Status](https://travis-ci.com/pyurin/exasol-scripts-golang.svg?branch=master)](https://travis-ci.com/pyurin/exasol-scripts-golang)

## Description

Golang exasol client allows running Go scripts on Exasol db.

Installation can be executed with simple script ($BUCKETFS_PATH should be without slashes as suffix and prefix):
```
#./install.sh $BUCKETFS_HOST_PORT $BUCKETFS_PATH $BUCKETFS_USER_PASS
./install.sh 192.168.1.172:2580 "default/go" "w:write"
```

## Prerequisites
Docker, curl and Exasol server is required. Tested on Mac & Centos.
All manipulations are expected to be done in exasol-scripts-golang root path.

###Content
1. Docker container
2. Scripts
3. Run on Exasol
4. Usage - basics
5. Usage - ExaIter class
6. Usage - external libraries

## 1. Docker container
### 1.1 Download current Exasol docker base image.
```
docker import http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/default/EXAClusterOS/{$EXASOL_OS_IMAGE} exasol-os-image;
```

Example:
```
docker import http://192.168.1.172:2580/default/EXAClusterOS/ScriptLanguages-2018-05-07.tar.gz  exasol-os-image;
```


### 1.2 Build required docker image based on Exasol's one
Dockerfile is based on local **exasol-os-image**
```
docker build . -t exasol-scripts-golang-image
```

### 1.3 Upload docker container
We need to upload docker container, not image (don't know why actually).
Run container:
```
docker run --name exasol-scripts-golang-container exasol-scripts-golang-image
```
Upload container:
```
docker export exasol-scripts-golang-container | curl -v -u {$EXASOL_BUCKETFS_USER}:{$EXASOL_BUCKETFS_PASS} -X PUT -T - http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/{$EXASOL_BUCKET_GOPATH}/GolangImage.tar.gz
```
Example:
```
docker export exasol-scripts-golang-container | curl -v -u w:1 -X PUT -T - http://192.168.1.172:2580/default/go/GolangImage.tar.gz
```
Stop and delete container:
```
docker stop exasol-scripts-golang-container && docker rm exasol-scripts-golang-container
```

Be aware that exasol unpacks container archive for ~2 minutes and bucketfs does not
give expected results within that time and files will be missing.

```
#for further work with docker image you can print
# docker stop exasol-scripts-golang-container; docker rm exasol-scripts-golang-container
# docker run --name exasol-scripts-golang-container -it -v `pwd`:/exasol-script-languages/ exasol-scripts-golang-image /bin/bash
# and `export GOPATH=$GOPATH:/exasol-script-languages/` in container
```

## 2. Upload scripts
```
curl -f -u {$EXASOL_BUCKETFS_USER}:{$EXASOL_BUCKETFS_PASS} -X PUT -T ./src/exago.sh http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/{$EXASOL_BUCKET_GOPATH}/src/exago.sh
curl -f -u {$EXASOL_BUCKETFS_USER}:{$EXASOL_BUCKETFS_PASS} -X PUT -T ./src/exago.go http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/{$EXASOL_BUCKET_GOPATH}/src/exago.go
tar -zc -C ./src/exago/ . | curl -f -u {$EXASOL_BUCKETFS_USER}:{$EXASOL_BUCKETFS_PASS} -X PUT -T - http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/{$EXASOL_BUCKET_GOPATH}/src/exago.tar.gz
```

Example:
```
curl -f -u w:1 -X PUT -T ./src/exago.sh http://192.168.1.172:2580/default/go/src/exago.sh
curl -f -u w:1 -X PUT -T ./src/exago.go http://192.168.1.172:2580/default/go/src/exago.go
tar -zc -C ./src/exago/ . | curl -f -u w:1 -X PUT -T - http://192.168.1.172:2580/default/go/src/exago.tar.gz
```


## 3. Run on exasol:
Add GO definition into SCRIPT_LANGUAGES param with:
```
GO=localzmq+protobuf:///bfsdefault/{$EXASOL_BUCKET_GOPATH}/GolangImage?#buckets/bfsdefault/{$EXASOL_BUCKET_GOPATH}/src/exago.sh
```

Example:
```
alter session set script_languages = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/src/exago.sh';
```

Run test go app
```
CREATE OR REPLACE GO  SCALAR SCRIPT gotest() RETURNS VARCHAR(100) AS

        package main

        import "exago"

        func Run(iter *exago.ExaIter) *string {
                s := "hello"
                return &s
        }

/
```
```
SELECT gotest();
```

If execution hangs, it's useful to run debug client:
install pyexasol [https://github.com/badoo/pyexasol/blob/master/docs/SCRIPT_OUTPUT.md] and run debugger like
```
python3 -m pyexasol script_debug --host 0.0.0.0 --p 2544
```
## 4. Usage - basics


Golang script must comply following templates:
In case of *RETURNS*:
```
        package main

        import "exago"

        func Run(iter *exago.ExaIter) *string {
        }
```
and in case of *EMITS*:
```
        package main

        import "exago"

        func Run(iter *exago.ExaIter) {
        }
```

#### 5. Usage - ExaIter class
####Database interaction is performed via exago.ExaIter.

##### func (iter *ExaIter) Next() bool
Iterator function. Returns falst if next row does not exist

##### func (iter *ExaIter) Size() uint64
Number of input rows

##### func (iter *ExaIter) Reset() bool
Resets iterator and reads first row

##### func (iter *ExaIter) ReadInt64(colI int) *int64
Reads int64

##### func (iter *ExaIter) ReadDecimalApd(colI int) *apd.Decimal
Reads decimal with scale as cockroachdb/apd

##### func (iter *ExaIter) ReadIntBig(colI int) *big.Int
Reads decimal w/o scale as math/big

##### func (iter *ExaIter) ReadInt32(colI int) *int32
Reads int32

##### func (iter *ExaIter) ReadBool(colI int) *bool
Reads bool

##### func (iter *ExaIter) ReadFloat64(colI int) *float64
Reads float64 / double

##### func (iter *ExaIter) ReadIsNull(colI int) bool
Checks if value is null

##### func (iter *ExaIter) ReadTime(colI int) *time.Time
Reads date or timestamp

##### func (iter *ExaIter) ReadString(colI int) *string
Reads varchar, decimal with scale, big decimal, date, timestamp values as string


##### func (iter *ExaIter) ReadColInt64(colName string) *int64
Reads int64

##### func (iter *ExaIter) ReadColDecimalApd(colName string) *apd.Decimal
Reads decimal with scale as cockroachdb/apd

##### func (iter *ExaIter) ReadColIntBig(colName string) *big.Int
Reads decimal w/o scale as math/big

##### func (iter *ExaIter) ReadColInt32(colName string) *int32
Reads int32

##### func (iter *ExaIter) ReadColBool(colName string) *bool
Reads bool

##### func (iter *ExaIter) ReadColFloat64(colName string) *float64
Reads float64 / double

##### func (iter *ExaIter) ReadColIsNull(colName string) bool
Checks if value is null

##### func (iter *ExaIter) ReadColTime(colName string) *time.Time
Reads date or timestamp

##### func (iter *ExaIter) ReadColString(colName string) *string
Reads varchar, decimal with scale, big decimal, date, timestamp values as string


##### func (iter *ExaIter) EmitInt64(i int64)
Emits int64

##### func (iter *ExaIter) EmitInt32(i int32)
Emits int32

##### func (iter *ExaIter) EmitBool(b bool)
Emits bool

##### func (iter *ExaIter) EmitTime(t time.Time)
Emits time / date

##### func (iter *ExaIter) EmitFloat64(f float64)
Emits float64 / double

##### func (iter *ExaIter) EmitNull()
Emits null value

##### func (iter *ExaIter) EmitString(s string)
Emits string

##### func (iter *ExaIter) EmitDecimalApd(d apd.Decimal)
Emits cockroachdb/apd - for decimal with scale or big decimals

##### func (iter *ExaIter) EmitIntBig(i big.Int)
Emits math/big int


Usage examples:
- get column by number (w/o map lookup)
```
CREATE OR REPLACE GO  SCALAR SCRIPT gotest(a DECIMAL(16,0)) RETURNS DECIMAL(16,0) AS

package main

import "exago"

func Run(iter *exago.ExaIter) *int64 {
    i := *iter.ReadInt64(0)
    i = i * i
    return &i;
}
/

SELECT gotest(12);
```

- emit
```
CREATE OR REPLACE GO  SCALAR SCRIPT gotest(a DECIMAL(16,0), b DECIMAL(16,0)) EMITS (v DECIMAL(16,0), i DECIMAL(16,0)) AS

package main

import "exago"

func Run(iter *exago.ExaIter) {
    var sumResult int64;
    for i := *iter.ReadInt64(0); i <= *iter.ReadInt64(1); i++ {
        sumResult += i;
        iter.EmitInt64(i)
        iter.EmitInt64(sumResult)
    }
}
/


SELECT gotest(1, 10);
```

- iterate
```
CREATE OR REPLACE GO SET SCRIPT gotest(a VARCHAR(100)) RETURNS VARCHAR(1025) AS

package main

import "exago"

func Run(iter *exago.ExaIter) *string {
    var resultS string;
    for true {
        resultS += *iter.ReadString(0)
        if !iter.Next() {
            break;
        }
    }
    return &resultS;
}
/

SELECT gotest(a) FROM (
    SELECT 'str1' as a
    UNION ALL
    SELECT 'str2' as a
    UNION ALL
    SELECT 'str3' as a
)
```


### 6. Usage - external libraries
External libraries usage is support by exago project - local fs packages and github packages.
Libraries must be uploaded into http://$EXASOL_HOST_PORT/$EXASOL_BUCKET_GOPATH/src/$PACKAGE_NAME folder and will be available
with ***import "$PACKAGE_NAME"*** directive.
For easy libraries upload you can use ***upload_lib_local.sh*** and ***upload_lib_hithub.sh*** scripts.
####Uploading local library
```
./upload_lib_local.sh $EXASOL_HOST_PORT $EXASOL_BUCKET_GOPATH $BUCKETFS_USER_PASS $PACKAGE_PATH
```
Example:
```
./upload_lib_local.sh 192.168.1.231:2580 "default/go" "w:1" ./test/testnumbers/
```
In that example ***testnumbers*** folder will be uploaded as http://192.168.1.231:2580/default/go/src/testnumbers.tar.gz
and will be available as ***import "testnumbers"***:
```
CREATE OR REPLACE GO SCALAR SCRIPT gotest(a DECIMAL(16,0)) RETURNS BOOLEAN AS

package main

import "exago"
import "testnumbers"

func Run(iter *exago.ExaIter) *bool {
    b := testnumbers.IsOdd(*iter.ReadInt64(0))
    return &b;
}
/

SELECT gotest(2015);
```
####Uploading github library
```
./upload_lib_github.sh $EXASOL_HOST_PORT $EXASOL_BUCKET_GOPATH $BUCKETFS_USER_PASS $GITHUB_URL
```
Example:
```
./upload_lib_github.sh 192.168.1.231:2580 "default/go" "w:1" https://github.com/visualfc/fibutil
```
IN that example library will be uploaded as http://192.168.1.231:2580/default/go/src/github.com/visualfc/fibutil and
available as ***import "github.com/visualfc/fibutil"*** or ***import "github.com/visualfc/fibutil/fib"***
```
CREATE OR REPLACE GO SCALAR SCRIPT test.gotest(a DECIMAL(16,0)) RETURNS DECIMAL(36, 0) AS

package main

import "exago"
import "math/big"
import "github.com/visualfc/fibutil/fib"

func Run(iter *exago.ExaIter) *big.Int {
    return fib.Fib(*iter.ReadInt64(0));
}
/

SELECT test.gotest(10);
```