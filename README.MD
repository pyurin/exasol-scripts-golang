## Description

Golang exasol client allows running golang scripts on Exasol db.

## Prerequisites
Docker and Exasol server is required. Tested on Mac & Centos.
All manipulations are expected to be done in exasol-scripts-golang root path.

1. Docker container
2. Scripts
3. Run on Exasol
4. Usage

## 1. Docker container
### 1.1 Download current Exasol docker base image.
```
docker import http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/default/EXAClusterOS/{$EXASOL_OS_IMAGE} exasol-golang-os-image;
```

Example:
```
docker import http://192.168.1.172:2580/default/EXAClusterOS/ScriptLanguages-2018-05-07.tar.gz  exasol-golang-os-container-base;
```


### 1.2 Build required docker image based on Exasol's one
Start docker container
```
docker stop exasol-scripts-golang-container; docker rm exasol-scripts-golang-container; docker run -v `pwd`/src/:/src/ -it --name exasol-scripts-golang-container exasol-golang-os-container-base /bin/bash;
```
and configure it running specific commands
```
chmod 777 /tmp &&
  apt-get update --allow-unauthenticated &&
  apt-get install -y pkg-config &&
  add-apt-repository -y ppa:gophers/archive &&
  apt-get update &&
  apt-get install -y golang-1.11-go --allow-unauthenticated &&

  echo "export GOCACHE=/tmp/go_cache/" >> ~/.bashrc &&
  echo "export GOPATH=/var/lib/go/" >> ~/.bashrc &&
  echo "export PATH=$PATH:/usr/lib/go-1.11/bin" >> ~/.bashrc &&

  echo "export GOCACHE=/tmp/go_cache/" >> /home/exasolution/.bashrc  &&
  echo "export GOPATH=/var/lib/go/" >> /home/exasolution/.bashrc &&
  echo "export PATH=$PATH:/usr/lib/go-1.11/bin" >> /home/exasolution/.bashrc &&
  source ~/.bashrc &&

  go get github.com/pebbe/zmq2 &&
  go install github.com/pebbe/zmq2 &&
  go get github.com/golang/protobuf/proto &&
  go install github.com/golang/protobuf/proto &&

  cp -R /src/* $GOPATH/src/ &&
  ln -s  /usr/lib/go-1.11 /usr/lib/go &&
  go build -i /src/*.go && rm /golauncher &&
  go build -i -buildmode=plugin /src/*.go && rm /golauncher.so &&

  chmod -R 0777 $GOPATH &&
  chmod -R 0777 $GOCACHE
;
```

### 1.3 Upload docker image
```
docker export exasol-scripts-golang-container | curl -v -u {$EXASOL_BUCKETFS_USER}:{$EXASOL_BUCKETFS_PASS} -X PUT -T - http://{$EXASOL_HOST}:{$EXASOL_BUCKETFS_PORT}/{$EXASOL_BUCKET_GOPATH}/GolangImage.tar.gz
```

Example:
```
docker export exasol-scripts-golang-container | curl -v -u w:1 -X PUT -T - http://192.168.1.172:2580/default/go/GolangImage.tar.gz
```

```
#for further work with docker image you can print
#	docker start exasol-scripts-golang-container && docker exec -it  exasol-scripts-golang-container /bin/bash
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
##### Row []interface{}
Current row columns


##### RowColumn map[string]*interface{}
Current row columns by name


##### func (iter *ExaIter) Emit(out ...interface{})
Emit a row


##### func (iter *ExaIter) Next() bool
Iterates input, returning false if no more rows to fetch


Usage examples:
- get column by number (w/o map lookup)
```
CREATE OR REPLACE GO  SCALAR SCRIPT gotest(a DECIMAL(16,0)) RETURNS DECIMAL(16,0) AS

package main

import "exago"

func Run(iter *exago.ExaIter) interface{} {
    i := iter.Row[0].(int64)
    return i * i;
}
/

SELECT gotest(12);
```

- get column by name
```
CREATE OR REPLACE GO  SCALAR SCRIPT gotest(a DECIMAL(16,0)) RETURNS DECIMAL(16,0) AS

package main

import "exago"

func Run(iter *exago.ExaIter) interface{} {
    i := (*iter.RowColumn["a"]).(int64)
    return i * i;
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
    for i := iter.Row[0].(int64); i < iter.Row[1].(int64); i++ {
        sumResult += i;
        iter.Emit(i, sumResult)
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

func Run(iter *exago.ExaIter) interface{} {
    var resultS string;
    for true {
        resultS += iter.Row[0].(string)
        if !iter.Next() {
            break;
        }
    }
    return resultS;
}
/

```