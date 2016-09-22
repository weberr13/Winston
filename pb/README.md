
installing protoc 3
```
go get -v -u github.com/gogo/protobuf/{proto,protoc-gen-gogo,gogoproto,protoc-gen-gofast};
cd /tmp
wget https://github.com/google/protobuf/releases/download/v3.0.2/protoc-3.0.2-linux-x86_64.zip
unzip protoc-3.0.2-linux-x86_64.zip
sudo cp -R include/* /usr/local/include/
sudo cp bin/* /usr/local/bin/
```
