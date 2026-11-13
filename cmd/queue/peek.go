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

type PeekOptions struct {
	cfg      *config.Config
	channel  string
	messages int
	wait     int
}

var queuePeekExamples = `
	# Peek 1 messages from a queue and wait for 2 seconds (default)
	skuta queue peek some-channel

	# Peek 3 messages from a queue and wait for 5 seconds
	skuta queue peek some-channel -m 3 -w 5
`
var queuePeekLong = `Queues peek command allows to peek one or many messages from a queue channel without removing them from the queue`
var queuePeekShort = `Queues peek a messages from a queue channel command`

func NewCmdQueuePeek(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &PeekOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "peek",
		Aliases: []string{"p"},
		Short:   queuePeekShort,
		Long:    queuePeekLong,
		Example: queuePeekExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().IntVarP(&o.messages, "messages", "m", 1, "set how many messages we want to peek from queue")
	cmd.PersistentFlags().IntVarP(&o.wait, "wait", "w", 2, "set how many seconds to wait for peeking queue messages")

	return cmd
}

func (o *PeekOptions) Complete(args []string) error {

	if len(args) >= 1 {
		o.channel = args[0]
		return nil
	}
	return fmt.Errorf("missing channel argument")
}

func (o *PeekOptions) Validate() error {
	return nil
}

func (o *PeekOptions) Run(ctx context.Context) error {
	client, err := grpcclient.New(o.cfg.Client)
	if err != nil {
		return fmt.Errorf("create skuta client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()
	res, err := client.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:           uuid.New(),
		ClientID:            client.ClientID(),
		Channel:             o.channel,
		MaxNumberOfMessages: int32(o.messages),
		WaitTimeSeconds:     int32(o.wait),
		IsPeak:              true,
	})
	if err != nil {
		return fmt.Errorf("peek queue message, %s", err.Error())
	}
	if res.IsError {
		return fmt.Errorf("peek queue message, %s", res.Error)
	}

	if res.MessagesReceived > 0 {
		utils.Printlnf("peeking %d messages", res.MessagesReceived)
		printItems(res.Messages)
	} else {
		utils.Printlnf("no messages in queue to peek")
	}

	return nil
}
