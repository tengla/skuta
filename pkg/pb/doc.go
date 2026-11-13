// Package pb is the KubeMQ gRPC protocol: the messages and the service that
// the KubeMQ SDKs speak, which is what makes them work against this server.
//
// kubemq_go.proto and kubemq_go.pb.go are copied from
// github.com/kubemq-io/protobuf v1.3.1 (directory go/, Apache-2.0), so the
// server builds without that repository. The only change is the package
// clause of the generated file, from kubemq to pb.
//
// The proto package and the service are both named kubemq, which puts
// /kubemq.kubemq/<Method> on the wire. Renaming either breaks every SDK.
//
// The code was generated with gogo/protobuf:
//
//	protoc --gofast_out=plugins=grpc:. kubemq_go.proto
//
// with gogo.proto (github.com/gogo/protobuf/gogoproto) on the import path.
package pb
