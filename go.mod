module github.com/qri-io/dataset

go 1.12

// override with corret timestamps for circleci: https://github.com/qri-io/qri/pull/865/files
replace (
	github.com/go-critic/go-critic v0.0.0-20181204210945-c3db6069acc5 => github.com/go-critic/go-critic v0.0.0-20190422201921-c3db6069acc5
	github.com/go-critic/go-critic v0.0.0-20181204210945-ee9bf5809ead => github.com/go-critic/go-critic v0.0.0-20190210220443-ee9bf5809ead
	github.com/golangci/errcheck v0.0.0-20181003203344-ef45e06d44b6 => github.com/golangci/errcheck v0.0.0-20181223084120-ef45e06d44b6
	github.com/golangci/go-tools v0.0.0-20180109140146-af6baa5dc196 => github.com/golangci/go-tools v0.0.0-20190318060251-af6baa5dc196
	github.com/golangci/gofmt v0.0.0-20181105071733-0b8337e80d98 => github.com/golangci/gofmt v0.0.0-20181222123516-0b8337e80d98
	github.com/golangci/gosec v0.0.0-20180901114220-66fb7fc33547 => github.com/golangci/gosec v0.0.0-20190211064107-66fb7fc33547
	github.com/golangci/lint-1 v0.0.0-20180610141402-ee948d087217 => github.com/golangci/lint-1 v0.0.0-20190420132249-ee948d087217
	mvdan.cc/unparam v0.0.0-20190124213536-fbb59629db34 => mvdan.cc/unparam v0.0.0-20190209190245-fbb59629db34
)

require (
	github.com/360EntSecGroup-Skylar/excelize v1.4.1
	github.com/google/go-cmp v0.3.0
	github.com/ipfs/go-datastore v0.1.0
	github.com/ipfs/go-log v0.0.1
	github.com/jinzhu/copier v0.0.0-20180308034124-7e38e58719c3
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/libp2p/go-libp2p-core v0.2.3
	github.com/mr-tron/base58 v1.1.2
	github.com/multiformats/go-multihash v0.0.8
	github.com/qri-io/compare v0.1.0
	github.com/qri-io/jsonschema v0.2.0
	github.com/qri-io/qfs v0.1.1-0.20191025195012-9971677b190d
	github.com/qri-io/varName v0.1.0
	github.com/ugorji/go/codec v1.1.7
	github.com/yudai/gojsondiff v1.0.0
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yudai/pp v2.0.1+incompatible // indirect
)
