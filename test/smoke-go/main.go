// Smoke test: the kubemq-go SDK against a running Skuta server.
//
//	go run . [host:port]        (default: $SKUTA_ADDRESS or localhost:50000)
//
// Set SKUTA_TOKEN when the server has authentication enabled, and
// SKUTA_TLS_CA to the CA file for a server with TLS. kubemq-go can't present a
// client certificate, so it can't reach a server that requires one (mTLS).
// SKUTA_REST_URL (for example http://localhost:9090) also runs the queue checks
// over the SDK's REST transport. Each run uses fresh channel names.
//
// This is its own Go module so that the server doesn't depend on the SDK: the
// SDK brings a second copy of the protocol, and two can't load in one process.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go"
	"github.com/kubemq-io/kubemq-go/queues_stream"
)

var run = strconv.FormatInt(time.Now().UnixMilli(), 36)

func ch(name string) string { return "smoke." + run + "." + name }

func main() {
	address := os.Getenv("SKUTA_ADDRESS")
	if len(os.Args) > 1 {
		address = os.Args[1]
	}
	if address == "" {
		address = "localhost:50000"
	}
	if err := smoke(address); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func smoke(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("address %q: %w", address, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("address %q: %w", address, err)
	}
	token := os.Getenv("SKUTA_TOKEN")
	var cert string
	if file := os.Getenv("SKUTA_TLS_CA"); file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("SKUTA_TLS_CA: %w", err)
		}
		cert = string(data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	options := []kubemq.Option{
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("smoke-go-" + run),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithCheckConnection(true),
	}
	if token != "" {
		options = append(options, kubemq.WithAuthToken(token))
	}
	if cert != "" {
		options = append(options, kubemq.WithCertificate(cert, ""))
	}
	client, err := kubemq.NewClient(ctx, options...)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", address, err)
	}
	defer func() { _ = client.Close() }()

	streamOptions := []queues_stream.Option{
		queues_stream.WithAddress(host, port),
		queues_stream.WithClientId("smoke-go-stream-" + run),
	}
	if token != "" {
		streamOptions = append(streamOptions, queues_stream.WithAuthToken(token))
	}
	if cert != "" {
		streamOptions = append(streamOptions, queues_stream.WithCertificate(cert, ""))
	}
	queues, err := queues_stream.NewQueuesStreamClient(ctx, streamOptions...)
	if err != nil {
		return fmt.Errorf("connect the queues stream client to %s: %w", address, err)
	}
	defer func() { _ = queues.Close() }()

	checks := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"ping", func(ctx context.Context) error { _, err := client.Ping(ctx); return err }},
		{"events: publish and subscribe", func(ctx context.Context) error { return events(ctx, client) }},
		{"events store: publish, then read from the first event", func(ctx context.Context) error { return eventsStore(ctx, client) }},
		{"commands: send and respond", func(ctx context.Context) error { return commands(ctx, client) }},
		{"queries: send and respond", func(ctx context.Context) error { return queries(ctx, client) }},
		{"queues: send, peek, receive and ack all", func(ctx context.Context) error { return queueBasics(ctx, client, "grpc") }},
		{"queues: stream receive, extend visibility and ack", func(ctx context.Context) error { return queueStream(ctx, client, "grpc") }},
		{"queues: stream reject moves to the dead-letter queue", func(ctx context.Context) error { return queueDeadLetter(ctx, client, "grpc") }},
		// The queues stream client keeps its streams on the context of its first
		// call, so these use the run's context and not the one of a single check.
		{"queues stream: send and poll with auto ack", func(context.Context) error {
			return poll(ctx, queues, true, func(*queues_stream.PollResponse) error { return nil }, "", 0)
		}},
		{"queues stream: poll and ack all", func(context.Context) error {
			return poll(ctx, queues, false, func(r *queues_stream.PollResponse) error { return r.AckAll() }, "", 0)
		}},
		{"queues stream: poll and nack all redelivers", func(context.Context) error {
			return poll(ctx, queues, false, func(r *queues_stream.PollResponse) error { return r.NAckAll() }, "", pollCount)
		}},
		{"queues stream: poll and requeue all moves the messages", func(context.Context) error {
			target := ch("poll.requeued")
			return poll(ctx, queues, false, func(r *queues_stream.PollResponse) error { return r.ReQueueAll(target) }, target, 0)
		}},
		{"queues stream: ack each message", func(context.Context) error {
			return poll(ctx, queues, false, func(r *queues_stream.PollResponse) error {
				for _, msg := range r.Messages {
					if err := msg.Ack(); err != nil {
						return err
					}
				}
				return nil
			}, "", 0)
		}},
		{"queues stream: a poll on an invalid channel fails", func(context.Context) error {
			_, err := queues.Poll(ctx, queues_stream.NewPollRequest().SetChannel(">").SetMaxItems(1).SetWaitTimeout(1000))
			if err == nil {
				return errors.New("no error")
			}
			return nil
		}},
	}

	if restURL := os.Getenv("SKUTA_REST_URL"); restURL != "" {
		restOptions := []kubemq.Option{
			kubemq.WithUri(restURL),
			kubemq.WithClientId("smoke-go-rest-" + run),
			kubemq.WithTransportType(kubemq.TransportTypeRest),
		}
		if token != "" {
			restOptions = append(restOptions, kubemq.WithAuthToken(token))
		}
		rest, err := kubemq.NewClient(ctx, restOptions...)
		if err != nil {
			return fmt.Errorf("connect to %s: %w", restURL, err)
		}
		defer func() { _ = rest.Close() }()
		checks = append(checks, []struct {
			name string
			fn   func(context.Context) error
		}{
			{"rest: queues send, peek, receive and ack all", func(ctx context.Context) error { return queueBasics(ctx, rest, "rest") }},
			{"rest: queues stream receive, extend visibility and ack", func(ctx context.Context) error { return queueStream(ctx, rest, "rest") }},
			{"rest: queues stream reject moves to the dead-letter queue", func(ctx context.Context) error { return queueDeadLetter(ctx, rest, "rest") }},
		}...)
	}

	failed := 0
	for _, check := range checks {
		checkCtx, checkCancel := context.WithTimeout(ctx, 20*time.Second)
		err := check.fn(checkCtx)
		checkCancel()
		if err != nil {
			failed++
			fmt.Printf("FAIL  %s: %v\n", check.name, err)
			continue
		}
		fmt.Printf("ok    %s\n", check.name)
	}
	fmt.Printf("\n%d of %d checks passed against %s (kubemq-go)\n", len(checks)-failed, len(checks), address)
	if failed > 0 {
		return fmt.Errorf("%d checks failed", failed)
	}
	return nil
}

func events(ctx context.Context, client *kubemq.Client) error {
	channel := ch("events")
	errCh := make(chan error, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	received, err := client.SubscribeToEvents(subCtx, channel, "", errCh)
	if err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	if err := client.E().SetChannel(channel).SetBody([]byte("hello")).Send(ctx); err != nil {
		return err
	}
	select {
	case event := <-received:
		return want("hello", string(event.Body))
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return errors.New("no event received")
	}
}

func eventsStore(ctx context.Context, client *kubemq.Client) error {
	channel := ch("events_store")
	for i := 1; i <= 3; i++ {
		res, err := client.ES().SetChannel(channel).SetBody([]byte(fmt.Sprintf("event %d", i))).Send(ctx)
		if err != nil {
			return err
		}
		if !res.Sent {
			return fmt.Errorf("event %d not stored: %v", i, res.Err)
		}
	}
	errCh := make(chan error, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	received, err := client.SubscribeToEventsStore(subCtx, channel, "", errCh, kubemq.StartFromFirstEvent())
	if err != nil {
		return err
	}
	for i := 1; i <= 3; i++ {
		select {
		case event := <-received:
			if err := want(fmt.Sprintf("event %d", i), string(event.Body)); err != nil {
				return err
			}
			if event.Sequence != uint64(i) {
				return fmt.Errorf("sequence %d, want %d", event.Sequence, i)
			}
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return fmt.Errorf("got %d of 3 events", i-1)
		}
	}
	return nil
}

func commands(ctx context.Context, client *kubemq.Client) error {
	channel := ch("commands")
	errCh := make(chan error, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	received, err := client.SubscribeToCommands(subCtx, channel, "", errCh)
	if err != nil {
		return err
	}
	go func() {
		select {
		case command := <-received:
			_ = client.R().SetRequestId(command.Id).SetResponseTo(command.ResponseTo).SetExecutedAt(time.Now()).Send(subCtx)
		case <-subCtx.Done():
		}
	}()
	time.Sleep(500 * time.Millisecond)
	res, err := client.C().SetChannel(channel).SetBody([]byte("do it")).SetTimeout(5 * time.Second).Send(ctx)
	if err != nil {
		return err
	}
	if !res.Executed {
		return fmt.Errorf("not executed: %s", res.Error)
	}
	return nil
}

func queries(ctx context.Context, client *kubemq.Client) error {
	channel := ch("queries")
	errCh := make(chan error, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	received, err := client.SubscribeToQueries(subCtx, channel, "", errCh)
	if err != nil {
		return err
	}
	go func() {
		select {
		case query := <-received:
			_ = client.R().SetRequestId(query.Id).SetResponseTo(query.ResponseTo).SetExecutedAt(time.Now()).
				SetBody(append([]byte("answer to "), query.Body...)).Send(subCtx)
		case <-subCtx.Done():
		}
	}()
	time.Sleep(500 * time.Millisecond)
	res, err := client.Q().SetChannel(channel).SetBody([]byte("question")).SetTimeout(5 * time.Second).Send(ctx)
	if err != nil {
		return err
	}
	if !res.Executed {
		return fmt.Errorf("not executed: %s", res.Error)
	}
	return want("answer to question", string(res.Body))
}

func queueBasics(ctx context.Context, client *kubemq.Client, transport string) error {
	channel := ch("queue.basics." + transport)
	batch := client.QMB()
	for i := 0; i < 10; i++ {
		batch.Add(client.QM().SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i))))
	}
	sent, err := batch.Send(ctx)
	if err != nil {
		return err
	}
	for _, res := range sent {
		if res.IsError {
			return errors.New(res.Error)
		}
	}
	peeked, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(5).SetWaitTimeSeconds(2).SetIsPeak(true).Send(ctx)
	if err != nil {
		return err
	}
	if err := wantCount("peeked", 5, len(peeked.Messages)); err != nil {
		return err
	}
	received, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(5).SetWaitTimeSeconds(2).Send(ctx)
	if err != nil {
		return err
	}
	if err := wantCount("received", 5, len(received.Messages)); err != nil {
		return err
	}
	acked, err := client.AQM().SetChannel(channel).SetWaitTimeSeconds(2).Send(ctx)
	if err != nil {
		return err
	}
	return wantCount("acked", 5, int(acked.AffectedMessages))
}

func queueStream(ctx context.Context, client *kubemq.Client, transport string) error {
	channel := ch("queue.stream." + transport)
	if _, err := client.QM().SetChannel(channel).SetBody([]byte("stream me")).Send(ctx); err != nil {
		return err
	}
	stream := client.SQM().SetChannel(channel)
	defer stream.Close()
	msg, err := stream.Next(ctx, 2, 5)
	if err != nil {
		return err
	}
	if err := want("stream me", string(msg.Body)); err != nil {
		return err
	}
	if err := msg.ExtendVisibility(3); err != nil {
		return fmt.Errorf("extend visibility: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	left, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	if err != nil {
		return err
	}
	return wantCount("left in the queue", 0, len(left.Messages))
}

func queueDeadLetter(ctx context.Context, client *kubemq.Client, transport string) error {
	channel := ch("queue.reject." + transport)
	deadLetter := ch("queue.dead." + transport)
	_, err := client.QM().SetChannel(channel).SetBody([]byte("reject me")).
		SetPolicyMaxReceiveCount(1).SetPolicyMaxReceiveQueue(deadLetter).Send(ctx)
	if err != nil {
		return err
	}
	stream := client.SQM().SetChannel(channel)
	msg, err := stream.Next(ctx, 2, 5)
	if err != nil {
		return err
	}
	if err := msg.Reject(); err != nil {
		return fmt.Errorf("reject: %w", err)
	}
	stream.Close()
	dead, err := client.RQM().SetChannel(deadLetter).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(5).Send(ctx)
	if err != nil {
		return err
	}
	return wantCount("in the dead-letter queue", 1, len(dead.Messages))
}

const pollCount = 10

var pollRuns int

// poll sends pollCount messages, polls them, settles them with settle and then
// checks how many are left in the queue and, if given, in movedTo.
func poll(ctx context.Context, queues *queues_stream.QueuesStreamClient, autoAck bool, settle func(*queues_stream.PollResponse) error, movedTo string, wantLeft int) error {
	pollRuns++
	channel := ch(fmt.Sprintf("poll.%d", pollRuns))
	var messages []*queues_stream.QueueMessage
	for i := 0; i < pollCount; i++ {
		messages = append(messages, queues_stream.NewQueueMessage().SetChannel(channel).SetBody([]byte(fmt.Sprintf("message %d", i))))
	}
	sent, err := queues.Send(ctx, messages...)
	if err != nil {
		return err
	}
	for _, res := range sent.Results {
		if res.IsError {
			return errors.New(res.Error)
		}
	}
	request := func(channel string, autoAck bool) *queues_stream.PollRequest {
		return queues_stream.NewPollRequest().SetChannel(channel).SetAutoAck(autoAck).SetMaxItems(pollCount).SetWaitTimeout(1000)
	}
	response, err := queues.Poll(ctx, request(channel, autoAck))
	if err != nil {
		return err
	}
	if err := wantCount("polled", pollCount, len(response.Messages)); err != nil {
		return err
	}
	if err := settle(response); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	left, err := queues.Poll(ctx, request(channel, true))
	if err != nil {
		return err
	}
	if err := wantCount("left in the queue", wantLeft, len(left.Messages)); err != nil {
		return err
	}
	if movedTo == "" {
		return nil
	}
	moved, err := queues.Poll(ctx, request(movedTo, true))
	if err != nil {
		return err
	}
	return wantCount("moved", pollCount, len(moved.Messages))
}

func want(expected, got string) error {
	if expected != got {
		return fmt.Errorf("got %q, want %q", got, expected)
	}
	return nil
}

func wantCount(what string, expected, got int) error {
	if expected != got {
		return fmt.Errorf("%d %s, want %d", got, what, expected)
	}
	return nil
}
