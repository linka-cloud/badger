module github.com/dgraph-io/badger/v4

go 1.21

toolchain go1.22.1

replace github.com/shaj13/raft => github.com/linka-cloud/raft v0.0.0-20240308124846-d6f05cc7b12f

require (
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dustin/go-humanize v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	github.com/google/flatbuffers v1.12.1
	github.com/klauspost/compress v1.15.15
	github.com/pkg/errors v0.9.1
	github.com/shaj13/raft v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v1.7.0
	github.com/stretchr/testify v1.8.4
	go.opencensus.io v0.22.5
	go.uber.org/multierr v1.11.0
	golang.org/x/net v0.22.0
	golang.org/x/sync v0.6.0
	golang.org/x/sys v0.18.0
	google.golang.org/grpc v1.62.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/glog v1.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.19.0 // indirect
	github.com/prometheus/client_model v0.6.0 // indirect
	github.com/prometheus/common v0.50.0 // indirect
	github.com/prometheus/procfs v0.13.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.12 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.12 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.12 // indirect
	go.etcd.io/etcd/server/v3 v3.5.12 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240304212257-790db918fca8 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v4.0.0 // see #1888 and #1889
