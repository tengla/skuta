package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nuid"
	"github.com/tengla/skuta/pkg/pb"
)

// queueClient calls the queue routes the way the KubeMQ SDKs' REST transport
// does: protobuf messages as JSON, answers in the Response envelope, and a
// websocket for queue streams. The SDKs themselves are checked by the smoke
// tests under test/, which run against a started server.
type queueClient struct {
	http  string
	ws    string
	id    string
	token string
}

func newQueueClient(port int, id string) *queueClient {
	if id == "" {
		id = nuid.Next()
	}
	return &queueClient{http: fmt.Sprintf("http://localhost:%d", port), ws: fmt.Sprintf("ws://localhost:%d", port), id: id}
}

func (c *queueClient) call(method, path string, body, out interface{}) error {
	res := &Response{}
	req := resty.New().SetAllowGetMethodPayload(true).R().SetBody(body).SetResult(res).SetError(res)
	if c.token != "" {
		req.SetHeader("Authorization", "Bearer "+c.token)
	}
	if _, err := req.Execute(method, c.http+path); err != nil {
		return err
	}
	if res.IsError {
		return errors.New(res.Message)
	}
	if out == nil || res.Data == nil {
		return nil
	}
	return json.Unmarshal(res.Data, out)
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

func (c *queueClient) send(msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	result := &pb.SendQueueMessageResult{}
	return result, c.call(http.MethodPost, "/queue/send", msg, result)
}

func (c *queueClient) sendBatch(messages []*pb.QueueMessage) ([]*pb.SendQueueMessageResult, error) {
	result := &pb.QueueMessagesBatchResponse{}
	err := c.call(http.MethodPost, "/queue/send_batch", &pb.QueueMessagesBatchRequest{BatchID: nuid.Next(), Messages: messages}, result)
	return result.Results, err
}

func (c *queueClient) receive(channel string, maxMessages, waitSeconds int32, peek bool) (*pb.ReceiveQueueMessagesResponse, error) {
	result := &pb.ReceiveQueueMessagesResponse{}
	return result, c.call(http.MethodPost, "/queue/receive", &pb.ReceiveQueueMessagesRequest{
		RequestID:           nuid.Next(),
		ClientID:            c.id,
		Channel:             channel,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     waitSeconds,
		IsPeak:              peek,
	}, result)
}

func (c *queueClient) ackAll(channel string, waitSeconds int32) (*pb.AckAllQueueMessagesResponse, error) {
	result := &pb.AckAllQueueMessagesResponse{}
	return result, c.call(http.MethodPost, "/queue/ack_all", &pb.AckAllQueueMessagesRequest{
		RequestID:       nuid.Next(),
		ClientID:        c.id,
		Channel:         channel,
		WaitTimeSeconds: waitSeconds,
	}, result)
}

func (c *queueClient) queuesInfo(filter string) (*pb.QueuesInfo, error) {
	result := &pb.QueuesInfoResponse{}
	err := c.call(http.MethodGet, "/queue/info", &pb.QueuesInfoRequest{RequestID: nuid.Next(), QueueName: filter}, result)
	return result.Info, err
}

// queueStream is one /queue/stream websocket: next receives a message, and
// ack, reject, resend or resendWithNewMessage settle it and close the socket.
type queueStream struct {
	client  *queueClient
	channel string
	conn    *websocket.Conn
	msg     *pb.QueueMessage
}

func (c *queueClient) stream(channel string) *queueStream {
	return &queueStream{client: c, channel: channel}
}

func (s *queueStream) next(visibilitySeconds, waitSeconds int32) (*pb.QueueMessage, error) {
	header := http.Header{}
	if s.client.token != "" {
		header.Add("Authorization", "Bearer "+s.client.token)
	}
	conn, _, err := websocket.DefaultDialer.Dial(s.client.ws+"/queue/stream", header)
	if err != nil {
		return nil, err
	}
	s.conn = conn
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
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return nil, err
	}
	_, data, err = s.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	res := &pb.StreamQueueMessagesResponse{}
	if err := json.Unmarshal(data, res); err != nil {
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
	if s.conn != nil {
		_ = s.conn.Close()
	}
}
