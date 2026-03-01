#!/bin/bash

# Run this script from its directory so proto paths resolve correctly.

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.0
protoc --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=paths=source_relative,features=marshal+unmarshal+size+clone:. \
  badgerpb4.proto wal_replication.proto
