package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nuid"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/pb"
	"google.golang.org/grpc"
)

// maxReceiveSize lifts gRPC's 4 MiB default for answers, as the SDKs do.
const maxReceiveSize = 100 << 20

// queueClient drives the queue RPCs the way the KubeMQ SDKs do, straight on
// the protobuf client. The SDKs themselves are checked by the smoke tests
// under test/, which run against a started server.
type queueClient struct {
	grpcClient
	id string
}

func newQueueClient(ctx context.Context, t *testing.T, appConfig *config.Config, id string) *queueClient {
	if id == "" {
		id = nuid.Next()
	}
	return &queueClient{grpcClient: getNewClient(ctx, t, appConfig), id: id}
}

func (c *queueClient) message(channel string, body []byte) *pb.QueueMessage {
	return &pb.QueueMessage{MessageID: nuid.Next(), ClientID: c.id, Channel: channel, Body: body}
}

func (c *queueClient) messages(channel string, amount int) []*pb.QueueMessage {
	list := make([]*pb.QueueMessage, 0, amount)
	for i := 0; i < amount; i++ {
		list = append(list, c.message(channel, []byte(fmt.Sprintf("msg #%d", i))))
	}
	return list
}

func (c *queueClient) sendBatch(ctx context.Context, messages []*pb.QueueMessage) ([]*pb.SendQueueMessageResult, error) {
	res, err := c.SendQueueMessagesBatch(ctx, &pb.QueueMessagesBatchRequest{BatchID: nuid.Next(), Messages: messages})
	if err != nil {
		return nil, err
	}
	return res.Results, nil
}

func (c *queueClient) receive(ctx context.Context, channel string, maxMessages, waitSeconds int32, peek bool) (*pb.ReceiveQueueMessagesResponse, error) {
	return c.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:           nuid.Next(),
		ClientID:            c.id,
		Channel:             channel,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     waitSeconds,
		IsPeak:              peek,
	}, grpc.MaxCallRecvMsgSize(maxReceiveSize))
}

func (c *queueClient) ackAll(ctx context.Context, channel string, waitSeconds int32) (*pb.AckAllQueueMessagesResponse, error) {
	return c.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
		RequestID:       nuid.Next(),
		ClientID:        c.id,
		Channel:         channel,
		WaitTimeSeconds: waitSeconds,
	})
}

// queueStream is one StreamQueueMessage call: next receives a message, and
// ack, reject, resend or resendWithNewMessage settle it and end the call.
type queueStream struct {
	client  *queueClient
	channel string
	stream  pb.Kubemq_StreamQueueMessageClient
	cancel  context.CancelFunc
	msg     *pb.QueueMessage
}

func (c *queueClient) stream(channel string) *queueStream {
	return &queueStream{client: c, channel: channel}
}

func (s *queueStream) next(ctx context.Context, visibilitySeconds, waitSeconds int32) (*pb.QueueMessage, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := s.client.StreamQueueMessage(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	s.stream, s.cancel = stream, cancel
	res, err := s.roundTrip(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
		Channel:               s.channel,
		VisibilitySeconds:     visibilitySeconds,
		WaitTimeSeconds:       waitSeconds,
	})
	if err != nil {
		s.close()
		return nil, err
	}
	s.msg = res.Message
	return res.Message, nil
}

func (s *queueStream) roundTrip(req *pb.StreamQueueMessagesRequest) (*pb.StreamQueueMessagesResponse, error) {
	req.RequestID = nuid.Next()
	req.ClientID = s.client.id
	if err := s.stream.Send(req); err != nil {
		return nil, err
	}
	res, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	if res.IsError {
		return nil, errors.New(res.Error)
	}
	return res, nil
}

func (s *queueStream) settle(req *pb.StreamQueueMessagesRequest) error {
	defer s.close()
	_, err := s.roundTrip(req)
	return err
}

func (s *queueStream) ack() error {
	return s.settle(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_AckMessage,
		Channel:               s.channel,
		RefSequence:           s.msg.Attributes.Sequence,
	})
}

func (s *queueStream) reject() error {
	return s.settle(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_RejectMessage,
		Channel:               s.channel,
		RefSequence:           s.msg.Attributes.Sequence,
	})
}

func (s *queueStream) extendVisibility(seconds int32) error {
	_, err := s.roundTrip(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
		Channel:               s.channel,
		VisibilitySeconds:     seconds,
	})
	return err
}

func (s *queueStream) resend(channel string) error {
	return s.settle(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_ResendMessage,
		Channel:               channel,
	})
}

func (s *queueStream) resendWithNewMessage(msg *pb.QueueMessage) error {
	return s.settle(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage,
		ModifiedMessage:       msg,
	})
}

func (s *queueStream) close() {
	if s.cancel != nil {
		s.cancel()
	}
}

// queuesStream sends on QueuesUpstream and polls on one QueuesDownstream
// stream. Every request of a poll goes on that one stream, as in the SDKs, so
// the server handles an ack before the poll that follows it.
type queuesStream struct {
	client *queueClient
	down   pb.Kubemq_QueuesDownstreamClient
	gets   chan *pb.QueuesDownstreamResponse
	cancel context.CancelFunc
}

func (c *queueClient) queuesStream(ctx context.Context) (*queuesStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	down, err := c.QueuesDownstream(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	q := &queuesStream{client: c, down: down, gets: make(chan *pb.QueuesDownstreamResponse, 1), cancel: cancel}
	go func() {
		for {
			res, err := down.Recv()
			if err != nil {
				return
			}
			if res.RequestTypeData != pb.QueuesDownstreamRequestType_Get {
				continue
			}
			select {
			case q.gets <- res:
			case <-streamCtx.Done():
				return
			}
		}
	}()
	return q, nil
}

func (q *queuesStream) close() {
	q.cancel()
}

func (q *queuesStream) send(ctx context.Context, messages []*pb.QueueMessage) (*pb.QueuesUpstreamResponse, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	up, err := q.client.QueuesUpstream(streamCtx)
	if err != nil {
		return nil, err
	}
	if err := up.Send(&pb.QueuesUpstreamRequest{RequestID: nuid.Next(), Messages: messages}); err != nil {
		return nil, err
	}
	res, err := up.Recv()
	if err != nil {
		return nil, err
	}
	if res.IsError {
		return nil, errors.New(res.Error)
	}
	return res, nil
}

// queuePoll is the answer to one poll, and the transaction that acks it.
type queuePoll struct {
	*pb.QueuesDownstreamResponse
	stream  *queuesStream
	channel string
}

func (q *queuesStream) poll(ctx context.Context, channel string, maxItems, waitTimeoutMs int32, autoAck bool) (*queuePoll, error) {
	err := q.down.Send(&pb.QueuesDownstreamRequest{
		RequestID:       nuid.Next(),
		ClientID:        q.client.id,
		RequestTypeData: pb.QueuesDownstreamRequestType_Get,
		Channel:         channel,
		MaxItems:        maxItems,
		WaitTimeout:     waitTimeoutMs,
		AutoAck:         autoAck,
	})
	if err != nil {
		return nil, err
	}
	select {
	case res := <-q.gets:
		if res.IsError {
			return nil, errors.New(res.Error)
		}
		return &queuePoll{QueuesDownstreamResponse: res, stream: q, channel: channel}, nil
	case <-time.After(time.Duration(waitTimeoutMs)*time.Millisecond + 10*time.Second):
		return nil, errors.New("timeout waiting for the poll response")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *queuePoll) request(kind pb.QueuesDownstreamRequestType, reQueueChannel string, sequences ...int64) error {
	return p.stream.down.Send(&pb.QueuesDownstreamRequest{
		RequestID:        nuid.Next(),
		ClientID:         p.stream.client.id,
		RequestTypeData:  kind,
		Channel:          p.channel,
		ReQueueChannel:   reQueueChannel,
		SequenceRange:    sequences,
		RefTransactionId: p.TransactionId,
	})
}

func (p *queuePoll) ackAll() error {
	return p.request(pb.QueuesDownstreamRequestType_AckAll, "")
}

func (p *queuePoll) nackAll() error {
	return p.request(pb.QueuesDownstreamRequestType_NAckAll, "")
}

func (p *queuePoll) reQueueAll(channel string) error {
	return p.request(pb.QueuesDownstreamRequestType_ReQueueAll, channel)
}

func (p *queuePoll) ack(msg *pb.QueueMessage) error {
	return p.request(pb.QueuesDownstreamRequestType_AckRange, "", int64(msg.Attributes.Sequence))
}

func (p *queuePoll) nack(msg *pb.QueueMessage) error {
	return p.request(pb.QueuesDownstreamRequestType_NAckRange, "", int64(msg.Attributes.Sequence))
}

func (p *queuePoll) reQueue(msg *pb.QueueMessage, channel string) error {
	return p.request(pb.QueuesDownstreamRequestType_ReQueueRange, channel, int64(msg.Attributes.Sequence))
}

func (p *queuePoll) closeByClient() error {
	return p.request(pb.QueuesDownstreamRequestType_CloseByClient, "")
}
