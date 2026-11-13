// Package grpcclient connects the CLI commands to a server over gRPC, using
// the protobuf service directly. It replaces the kubemq-go SDK, which can't
// present a client certificate to a server in mTLS mode.
package grpcclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/pb"
	"github.com/tengla/skuta/pkg/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// maxMessageSize matches the server's default body limit.
const maxMessageSize = 100 * 1024 * 1024

// reconnectInterval is how long Receive waits before reopening a stream to a
// server that went away.
const reconnectInterval = time.Second

type Client struct {
	pb.KubemqClient
	conn     *grpc.ClientConn
	clientID string
}

// New connects to cfg.GrpcHost:cfg.GrpcPort. It sends cfg.AuthToken with
// every call and uses TLS when cfg.TLS is enabled. The connection is made on
// the first call, so New doesn't fail when the server is down.
func New(cfg *config.ClientConfig) (*Client, error) {
	transport := insecure.NewCredentials()
	tlsConfig, err := cfg.TLS.Config()
	if err != nil {
		return nil, fmt.Errorf("client tls: %w", err)
	}
	if tlsConfig != nil {
		transport = credentials.NewTLS(tlsConfig)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	}
	if cfg.AuthToken != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(token(cfg.AuthToken)))
	}
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", cfg.GrpcHost, cfg.GrpcPort), opts...)
	if err != nil {
		return nil, err
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = uuid.New()
	}
	return &Client{KubemqClient: pb.NewKubemqClient(conn), conn: conn, clientID: clientID}, nil
}

// ClientID is the client ID to put in requests: client.clientId, or a random
// one per process.
func (c *Client) ClientID() string {
	return c.clientID
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Respond answers a command or query. A nil execErr marks it executed;
// otherwise the sender gets execErr as the error.
func (c *Client) Respond(ctx context.Context, req *pb.Request, body []byte, execErr error) error {
	res := &pb.Response{
		ClientID:     c.clientID,
		RequestID:    req.RequestID,
		ReplyChannel: req.ReplyChannel,
		Body:         body,
		Timestamp:    time.Now().Unix(),
		Executed:     execErr == nil,
	}
	if execErr != nil {
		res.Error = execErr.Error()
	}
	_, err := c.SendResponse(ctx, res)
	return err
}

// token sends the auth token the way the SDKs do, in the authorization
// metadata.
type token string

func (t token) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": string(t)}, nil
}

func (t token) RequireTransportSecurity() bool {
	return false
}

// Stream is the receiving side of a server stream.
type Stream[T any] interface {
	Recv() (T, error)
}

// Receive opens a stream and passes each message to handle until ctx ends.
// If the server goes away, by becoming unavailable or by ending the stream as
// it shuts down, Receive reopens the stream every second until the server is
// back. Any other error, from the stream or from handle, is returned.
//
// It pings the server first, so a server that can't be reached at all, or a
// TLS setup it rejects, fails at once instead of being retried forever.
func Receive[T any](ctx context.Context, c *Client, open func(context.Context) (Stream[T], error), handle func(T) error) error {
	if _, err := c.Ping(ctx, &pb.Empty{}); err != nil {
		return err
	}
	for {
		stream, err := open(ctx)
		for err == nil {
			var msg T
			if msg, err = stream.Recv(); err == nil {
				if err := handle(msg); err != nil {
					return err
				}
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if !errors.Is(err, io.EOF) && status.Code(err) != codes.Unavailable {
			return err
		}
		_, _ = fmt.Fprintf(os.Stderr, "server disconnected, reconnecting: %s\n", status.Convert(err).Message())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectInterval):
		}
	}
}
