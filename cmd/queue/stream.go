package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/cmd/internal/grpcclient"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
	"github.com/tengla/skuta/pkg/uuid"
)

type StreamOptions struct {
	cfg        *config.Config
	channel    string
	visibility int
	wait       int
}

var queueStreamExamples = `
	# Stream 'queues' message in transaction mode
	skuta queue stream q1

	# Stream 'queues' message in transaction mode with visibility set to 120 seconds and wait time of 180 seconds
	skuta queue stream q1 -v 120 -w 180
`
var queueStreamLong = `Queues stream command allows to receive message from a queue in push mode response an appropriate action`
var queueStreamShort = `Queues stream a message from a queue command`

func NewCmdQueueStream(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &StreamOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "stream",
		Aliases: []string{"st"},
		Short:   queueStreamShort,
		Long:    queueStreamLong,
		Example: queueStreamExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().IntVarP(&o.visibility, "visibility", "v", 30, "set initial visibility seconds")
	cmd.PersistentFlags().IntVarP(&o.wait, "wait", "w", 60, "set how many seconds to wait for 'queues' messages")

	return cmd
}

func (o *StreamOptions) Complete(args []string) error {
	if len(args) >= 1 {
		o.channel = args[0]
		return nil
	}
	return fmt.Errorf("missing channel argument")
}

func (o *StreamOptions) Validate() error {
	return nil
}

// errAbort ends the stream command without an error.
var errAbort = errors.New("aborted")

func (o *StreamOptions) Run(ctx context.Context) error {
	client, err := grpcclient.New(o.cfg.Client)
	if err != nil {
		return fmt.Errorf("create skuta client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	for {
		utils.Printlnf("waiting for the message in the queue: (waiting for %d seconds, visibility set to %d seconds)", o.wait, o.visibility)
		err := o.transaction(ctx, client)
		if errors.Is(err, errAbort) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// transaction receives one message on its own stream and applies the
// actions the user picks until one of them settles it. Ending the stream
// before that returns the message to the queue.
func (o *StreamOptions) transaction(ctx context.Context, client *grpcclient.Client) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StreamQueueMessage(ctx)
	if err != nil {
		return err
	}
	requestID := uuid.New()
	request := func(req *pb.StreamQueueMessagesRequest) (*pb.StreamQueueMessagesResponse, error) {
		req.RequestID = requestID
		req.ClientID = client.ClientID()
		if err := stream.Send(req); err != nil {
			return nil, err
		}
		res, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if res.IsError {
			return nil, errors.New(res.Error)
		}
		return res, nil
	}

	res, err := request(&pb.StreamQueueMessagesRequest{
		StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
		Channel:               o.channel,
		VisibilitySeconds:     int32(o.visibility),
		WaitTimeSeconds:       int32(o.wait),
	})
	if err != nil {
		return err
	}
	msg := res.Message
	if msg == nil {
		return errors.New("no new queue message available")
	}
	utils.Printlnf("[channel: %s] [client id: %s] -> {id: %s, metadata: %s, body: %s}", msg.Channel, msg.ClientID, msg.MessageID, msg.Metadata, msg.Body)
	var sequence uint64
	if msg.Attributes != nil {
		sequence = msg.Attributes.Sequence
	}

	for {
		action, result, err := o.prompt()
		if err != nil {
			return err
		}
		switch action {
		case "Ack":
			if _, err := request(&pb.StreamQueueMessagesRequest{StreamRequestTypeData: pb.StreamRequestType_AckMessage, Channel: o.channel, RefSequence: sequence}); err != nil {
				return err
			}
			utils.Println("Message Acked")
			return nil
		case "Reject":
			if _, err := request(&pb.StreamQueueMessagesRequest{StreamRequestTypeData: pb.StreamRequestType_RejectMessage, Channel: o.channel, RefSequence: sequence}); err != nil {
				return err
			}
			utils.Println("Message Rejected")
			return nil
		case "Extend visibility":
			val, err := strconv.Atoi(result)
			if err != nil {
				return err
			}
			if _, err := request(&pb.StreamQueueMessagesRequest{StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility, Channel: o.channel, VisibilitySeconds: int32(val)}); err != nil {
				return err
			}
			utils.Printlnf("Visibility Extended By %s Seconds.", result)
		case "Resend to another queue":
			if _, err := request(&pb.StreamQueueMessagesRequest{StreamRequestTypeData: pb.StreamRequestType_ResendMessage, Channel: result}); err != nil {
				return err
			}
			utils.Printlnf("Message Resent to %s.", result)
			return nil
		case "Ack and send new message":
			pair := strings.Split(result, ",")
			if len(pair) != 2 {
				return fmt.Errorf("invalid queue-name,message-body format")
			}
			newMessage := &pb.QueueMessage{ClientID: client.ClientID(), Channel: pair[0], Body: []byte(pair[1])}
			if _, err := request(&pb.StreamQueueMessagesRequest{StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage, ModifiedMessage: newMessage}); err != nil {
				return err
			}
			utils.Println("New Message Sent.")
			return nil
		case "Abort":
			utils.Println("Aborting.")
			return errAbort
		}
	}
}

func (o *StreamOptions) prompt() (string, string, error) {
	action := ""
	prompt := &survey.Select{
		Message: "What next:",
		Options: []string{"Ack", "Reject", "Extend visibility", "Resend to another queue", "Ack and send new message", "Abort"},
	}
	err := survey.AskOne(prompt, &action)
	if err != nil {
		return "", "", err
	}
	switch action {
	case "Ack", "Reject", "Abort":
		return action, "", nil
	case "Extend visibility":
		visibility := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "How long to extend visibility",
			Default:  "60",
			Help:     "In seconds",
		}
		err := survey.AskOne(prompt, &visibility)
		if err != nil {
			return "", "", err
		}
		return action, visibility, nil
	case "Resend to another queue":
		queueName := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "New queue name:",
			Default:  "new-queue",
			Help:     "",
		}
		err := survey.AskOne(prompt, &queueName, survey.WithValidator(survey.MinLength(1)))
		if err != nil {
			return "", "", err
		}
		return action, queueName, nil
	case "Ack and send new message":
		newMessage := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "New Message:",
			Default:  "new-queue,new-message",
			Help:     "Format queue-name,message-body ",
		}
		err := survey.AskOne(prompt, &newMessage, survey.WithValidator(survey.MinLength(1)))
		if err != nil {
			return "", "", err
		}
		return action, newMessage, nil
	}
	return "", "", fmt.Errorf("invalid input")
}
