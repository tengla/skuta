package queue

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tengla/skuta/cmd/internal/grpcclient"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/pb"
	"github.com/tengla/skuta/pkg/utils"
	"github.com/tengla/skuta/pkg/uuid"
)

type ReceiveOptions struct {
	cfg      *config.Config
	channel  string
	messages int
	wait     int
	watch    bool
}

var queueReceiveExamples = `
	# Receive 1 messages from a queue channel q1 and wait for 2 seconds (default)
	skuta queue receive q1

	# Receive 3 messages from a queue channel and wait for 5 seconds
	skuta queue receive q1 -m 3 -t 5

	# Watching 'queues' channel messages
	skuta queue receive q1 -w
`
var queueReceiveLong = `Queues receive command allows to receive one or many messages from a queue channel`
var queueReceiveShort = `Queues receive a messages from a queue channel command`

func NewCmdQueueReceive(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ReceiveOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "receive",
		Aliases: []string{"r", "rec", "subscribe", "sub"},
		Short:   queueReceiveShort,
		Long:    queueReceiveLong,
		Example: queueReceiveExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().IntVarP(&o.messages, "messages", "m", 1, "set how many messages we want to get from a queue")
	cmd.PersistentFlags().IntVarP(&o.wait, "wait-timeout", "t", 2, "set how many seconds to wait for 'queues' messages")
	cmd.PersistentFlags().BoolVarP(&o.watch, "watch", "w", false, "set watch on 'queues' channel")

	return cmd
}

func (o *ReceiveOptions) Complete(args []string) error {
	if len(args) >= 1 {
		o.channel = args[0]
		return nil
	}
	return fmt.Errorf("missing channel argument")
}

func (o *ReceiveOptions) Validate() error {
	return nil
}

func (o *ReceiveOptions) Run(ctx context.Context) error {
	if o.watch {
		utils.Printlnf("Watching %s 'queues' channel, waiting for messages...", o.channel)
	} else {
		utils.Printlnf("Pulling %d messages from %s 'queues' channel, waiting for %d seconds...", o.messages, o.channel, o.wait)
	}
	client, err := grpcclient.New(o.cfg.Client)
	if err != nil {
		return fmt.Errorf("create skuta client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	for {
		res, err := client.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
			RequestID:           uuid.New(),
			ClientID:            client.ClientID(),
			Channel:             o.channel,
			MaxNumberOfMessages: int32(o.messages),
			WaitTimeSeconds:     int32(o.wait),
		})
		if err != nil {
			return fmt.Errorf("receive 'queues' messages, %s", err.Error())
		}
		if res.IsError {
			return fmt.Errorf("receive 'queues' messages %s", res.Error)
		}

		if res != nil && res.MessagesReceived > 0 {
			printItems(res.Messages)
		} else if !o.watch {
			utils.Println("No new messages in queue")

		}
		if !o.watch {
			return nil
		}
	}

}
