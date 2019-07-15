## Description

Golang exasol client allows running golang scripts on Exasol db.

Stage 1 and 2 can be executed withscript just like that ($BUCKETFS_PATH should be without slashes as suffix and prefix):
```
#./easyinstall.sh $BUCKETFS_HOST_PORT $BUCKETFS_PATH $BUCKETFS_USER_PASS
./easyinstall.sh 192.168.1.172:2580 "default/go" "w:write"
```

## Prerequisites
Docker, curl and Exasol server is required. Tested on Mac & Centos.
All manipulations are expected to be done in exasol-scripts-golang root path.

1. Docker container
2. Scripts
3. Run on Exasol
4. Usage

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

Be aware that exasol unpacks container archive for ~a minute and bucketfs does not
give expected results within that time and files will be missing.

```
#for further work with docker image you can print
# docker stop exasol-scripts-golang-container; docker rm exasol-scripts-golang-container
# docker run --name exasol-scripts-golang-container -it -v `pwd`:/exasol-script-languages/ exasol-scripts-golang-image /bin/bash
# and `export GOPATH=$GOPATH:/exasol-script-languages/` in container
```

## 2. Upload scripts
```
tar -zc ./src/ ./go.sh | curl -v -u {$EXASOL_BUCKETFS_USER}:{$EXASOL_BUCKETFS_PASS} -X PUT -T - http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/{$EXASOL_BUCKET_GOPATH}/go_entrypoint.tar.gz
```

Example:
```
tar -zc ./src/ ./go.sh | curl -v -u w:1 -X PUT -T - http://192.168.1.172:2580/default/go/go_entrypoint.tar.gz
```


## 3. Run on exasol:
Add GO definition into SCRIPT_LANGUAGES param with:
```
GO=localzmq+protobuf:///bfsdefault/{$EXASOL_BUCKET_GOPATH}/GolangImage?#buckets/bfsdefault/{$EXASOL_BUCKET_GOPATH}/go_entrypoint/go.sh
```

Example:
```
alter session set script_languages = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/go/GolangImage?#buckets/bfsdefault/default/go/go_entrypoint/go.sh';
```

Run test go app
```
CREATE OR REPLACE GO  SCALAR SCRIPT gotest() RETURNS VARCHAR(100) AS

        package main

        import "exago"

        func Run(iter *exago.ExaIter) interface{} {
                return "hello"
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
## 4 Usage


Golang script must comply following templates:
In case of *RETURNS*:
```
        package main

        import "exago"

        func Run(iter *exago.ExaIter) interface{} {
        }
```
and in case of *EMITS*:
```
        package main

        import "exago"

        func Run(iter *exago.ExaIter) {
        }
```

####Database interaction is performed via exago.ExaIter.
####Usage:

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