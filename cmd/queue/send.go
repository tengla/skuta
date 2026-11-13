package queue

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tengla/skuta/cmd/internal/grpcclient"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/pb"
	"github.com/tengla/skuta/pkg/utils"
)

type SendOptions struct {
	cfg        *config.Config
	expiration int
	delay      int
	channel    string
	body       string
	maxReceive int
	metadata   string
	deadLetter string
	messages   int
}

var queueSendExamples = `
	# Send message to a queue channel channel
	skuta queue send q1 some-message
	
	# Send message to a queue channel with metadata
	skuta queue send q1 some-message --metadata some-metadata
	
	# Send 5 messages to a queues channel with metadata
	skuta queue send q1 some-message --metadata some-metadata -m 5
	
	# Send message to a queue channel with a message expiration of 5 seconds
	skuta queue send q1 some-message -e 5

	# Send message to a queue channel with a message delay of 5 seconds
	skuta queue send q1 some-message -d 5

	# Send message to a queue channel with a message policy of max receive 5 times and dead-letter queue 'dead-letter'
	skuta queue send q1 some-message -r 5 -q dead-letter
`
var queueSendLong = `Queues send command allows to send one or many message to a queue channel`
var queueSendShort = `Queues send a message to a queue channel command`

func NewCmdQueueSend(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &SendOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{
		Use:        "send",
		Aliases:    []string{"s"},
		SuggestFor: nil,
		Short:      queueSendShort,
		Long:       queueSendLong,
		Example:    queueSendExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().IntVarP(&o.expiration, "expiration", "e", 0, "set queue message expiration seconds")
	cmd.PersistentFlags().IntVarP(&o.delay, "delay", "d", 0, "set queue message sending delay seconds")
	cmd.PersistentFlags().IntVarP(&o.maxReceive, "max-receive", "r", 0, "set dead-letter max receive count")
	cmd.PersistentFlags().IntVarP(&o.messages, "messages", "i", 1, "set how many messages to send in a batch")
	cmd.PersistentFlags().StringVarP(&o.deadLetter, "dead-letter-queue", "q", "", "set dead-letter queue name")
	cmd.PersistentFlags().StringVarP(&o.metadata, "metadata", "m", "", "set queue message metadata field")

	return cmd
}

func (o *SendOptions) Complete(args []string) error {
	if len(args) >= 1 {
		o.channel = args[0]

	} else {
		return fmt.Errorf("missing channel argument")
	}
	if len(args) >= 2 {
		o.body = args[1]
	} else {
		return fmt.Errorf("missing body argument")
	}
	return nil
}

func (o *SendOptions) Validate() error {
	return nil
}

func (o *SendOptions) Run(ctx context.Context) error {
	client, err := grpcclient.New(o.cfg.Client)
	if err != nil {
		return fmt.Errorf("create skuta client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	for i := 0; i < o.messages; i++ {
		msg := &pb.QueueMessage{
			ClientID: client.ClientID(),
			Channel:  o.channel,
			Metadata: o.metadata,
			Body:     []byte(o.body),
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: int32(o.expiration),
				DelaySeconds:      int32(o.delay),
				MaxReceiveCount:   int32(o.maxReceive),
				MaxReceiveQueue:   o.deadLetter,
			},
		}
		utils.Println("Sending Queue Message:")
		printQueueMessage(msg)
		res, err := client.SendQueueMessage(ctx, msg)
		if err != nil {
			return fmt.Errorf("error sending queue message, %s", err.Error())
		}
		if res != nil {
			utils.Println("Response:")
			printQueueMessageResult(res)
		}
	}

	return nil
}
