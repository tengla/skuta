package grpc

import (
	"fmt"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/interfaces/grpc/middleware/recovery"
	"github.com/tengla/skuta/pkg/guard"
	"github.com/tengla/skuta/pkg/tracing"
	"github.com/tengla/skuta/services"
	"go.uber.org/atomic"
	"net"

	"github.com/tengla/skuta/pkg/entities"

	"github.com/pkg/errors"

	"github.com/tengla/skuta/pkg/logging"

	"github.com/tengla/skuta/interfaces/grpc/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

type options struct {
	port     int
	security *config.SecurityConfig
	maxSize  int
	bufSize  int
	guard    *guard.Guard
}

func recoveryFunc(p interface{}) (err error) {
	fmt.Println("crashed")
	return errors.Wrapf(entities.ErrGRPCSServerCrushed, " value: %v", p)
}
func configureServer(svc *services.SystemServices, logger *logging.Logger, opts *options, acceptTraffic *atomic.Bool) (s *grpc.Server, err error) {
	recoveryOpts := []recovery.Option{
		recovery.WithRecoveryHandler(recoveryFunc),
	}
	var connOptions []grpc.ServerOption
	connOptions = append(connOptions, grpc.MaxRecvMsgSize(opts.maxSize))
	connOptions = append(connOptions, grpc.MaxSendMsgSize(opts.maxSize))

	tlsConfig, err := opts.security.TLSConfig()
	if err != nil {
		return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "%s", err.Error())
	}
	if tlsConfig != nil {
		connOptions = append(connOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	streamInterceptors := []grpc.StreamServerInterceptor{
		recovery.StreamServerInterceptor(recoveryOpts...),
		middleware.StreamLoggerServerInterceptor(logger),
		middleware.StreamTrafficServerInterceptor(acceptTraffic),
	}
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		recovery.UnaryServerInterceptor(recoveryOpts...),
		middleware.UnaryLoggerServerInterceptor(logger),
		middleware.UnaryTrafficServerInterceptor(acceptTraffic),
	}
	if opts.guard != nil {
		streamInterceptors = append(streamInterceptors, middleware.StreamAuthServerInterceptor(opts.guard))
		unaryInterceptors = append(unaryInterceptors, middleware.UnaryAuthServerInterceptor(opts.guard))
	}
	if tracing.Enabled() {
		connOptions = append(connOptions, tracing.GRPCServerOption())
	}
	connOptions = append(connOptions, grpc.StreamInterceptor(middleware.ChainStreamServer(streamInterceptors...)))
	connOptions = append(connOptions, grpc.UnaryInterceptor(middleware.ChainUnaryServer(unaryInterceptors...)))

	s = grpc.NewServer(connOptions...)

	reflection.Register(s)

	return
}

func runServer(s *grpc.Server, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	go func() {
		_ = s.Serve(lis)
	}()

	return nil
}
