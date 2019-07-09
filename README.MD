
Build golang os image

   docker import http://192.168.1.172:2580/default/EXAClusterOS/ScriptLanguages-2018-05-07.tar.gz exasol-golang-os-image;

   docker stop exasol-golang-os-container && docker rm exasol-golang-os-container && docker run -v `pwd`/src/:/src/ -it --name exasol-golang-os-container exasol-golang-os-image /bin/bash;

   chmod 777 /tmp &&
	      apt-get update --allow-unauthenticated &&
	      apt-get install -y pkg-config &&
	      wget https://storage.googleapis.com/golang/go1.11.2.linux-amd64.tar.gz &&
	      tar -xvf go1.11.2.linux-amd64.tar.gz &&
	      mv go /usr/local &&

	      echo "export GOCACHE=/tmp/go_cache/" >> ~/.bashrc &&
	      echo "export GOPATH=/var/lib/go/" >> ~/.bashrc &&
	      echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc &&

	      echo "export GOCACHE=/tmp/go_cache/" >> /home/exasolution/.bashrc  &&
	      echo "export GOPATH=/var/lib/go/" >> /home/exasolution/.bashrc &&
	      echo "export PATH=$PATH:/usr/local/go/bin" >> /home/exasolution/.bashrc &&
	      source ~/.bashrc &&

	      go get github.com/pebbe/zmq2 &&
	      go install github.com/pebbe/zmq2 &&
	      go get github.com/golang/protobuf/proto &&
	      go install github.com/golang/protobuf/proto &&

          ln -s /var/lib/go/pkg /pkg &&
	      GOPATH=$GOPATH:/ go build -i /src/*.go && rm /golauncher &&
	      GOPATH=$GOPATH:/ go build -i -buildmode=plugin /src/*.go && rm /golauncher.so &&

	      chmod -R 0777 $GOPATH &&
	      chmod -R 0777 $GOCACHE
      ;


	docker export exasol-golang-os-container -o ./GolangImage.tar.gz &&
        curl -v -u w:1 -X PUT -T ./GolangImage.tar.gz http://192.168.1.172:2580/default/EXAClusterOS/GolangImage.tar.gz &&
        rm ./GolangImage.tar.gz


    #for further work with docker image
    #	docker start exasol-golang-os-container && docker exec -it  exasol-golang-os-container /bin/bash






Pack and put separately:
   scripts w/o precompilation:
       tar -zcf go_entrypoint.tar.gz ./src/ ./go.sh && curl -v -u w:1 -X PUT -T ./go_entrypoint.tar.gz http://192.168.1.172:2580/default/go/entrypoint.tar.gz && rm go_entrypoint.tar.gz

   python client
   		cd ~/dev/script-languages-master/python_client && tar -zcf mypythonclient.tar.gz ./*.py ./python3 && curl -v -u w:1 -X PUT -T ./mypythonclient.tar.gz http://192.168.1.172:2580/default/mypythonclient.tar.gz


alter session set script_languages = 'PYTHON=builtin_python R=builtin_r JAVA=builtin_java GO=localzmq+protobuf:///bfsdefault/default/EXAClusterOS/GolangImage?lang=test#buckets/bfsdefault/default/go/entrypoint/go.sh';



