module github.com/andrey-x-code/latencystorm/src

go 1.26.0

toolchain go1.26.10

replace github.com/andrey-x-code/latencystorm/contracts/go => ../contracts/go

require (
	github.com/andrey-x-code/latencystorm/contracts/go v0.0.0
	github.com/google/uuid v1.6.0
	github.com/segmentio/kafka-go v0.4.49
	google.golang.org/grpc v1.72.1
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
)
