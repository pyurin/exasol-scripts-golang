FROM exasol-os-image
RUN \
  chmod 777 /tmp && \
  apt-get update --allow-unauthenticated && \
  apt-get install -y pkg-config && \
  add-apt-repository -y ppa:gophers/archive && \
  apt-get update && \
  apt-get install -y golang-1.11-go --allow-unauthenticated && \
  ln -s  /usr/lib/go-1.11 /usr/lib/go

ENV GOCACHE="/tmp/go_cache/"
ENV GOPATH="/var/lib/go/"
ENV PATH="${PATH}:/usr/lib/go/bin"

COPY src/exago /var/lib/go/src/exago
COPY src/zmqcontainer /var/lib/go/src/zmqcontainer
COPY src/golauncher.go /tmp/golauncher.go

RUN \
  go get github.com/cockroachdb/apd && \
  go install github.com/cockroachdb/apd &&\
  go get github.com/pebbe/zmq4 && \
  go install github.com/pebbe/zmq4 && \
  go get github.com/golang/protobuf/proto && \
  go install github.com/golang/protobuf/proto && \
  go build -i /tmp/golauncher.go && rm /golauncher && \
  go build -i -buildmode=plugin /tmp/golauncher.go && rm /golauncher.so &&\
  chmod -R 0777 /tmp/go_cache/ && \
  chmod -R 0777 /var/lib/go/ && \
  rm -rf /var/lib/go/src/exago && \
  rm -rf /var/lib/go/src/zmqcontainer && \
  rm -rf /tmp/golauncher.go

CMD /bin/sh