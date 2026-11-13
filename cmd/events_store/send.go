package events_store

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

type SendOptions struct {
	cfg      *config.Config
	channel  string
	body     string
	metadata string
	messages int
}

var eventsSendExamples = `
	# Send (Publish) body to an 'events store' channel
	skuta events_store send some-channel some-body
	
	# Send (Publish) body to an 'events store' channel with metadata
	skuta events_store send some-channel some-body --metadata some-metadata

	# Send 10 messages to an 'events store' channel
	skuta events_store send some-channel some-body -m 10

	# Send 100 messages to an 'events store' channel in stream mode
	skuta events_store send some-channel some-body -m 100 -s
`
var eventsSendLong = `Events-Store send command allows to send (publish) one or many messages to an 'events store' channel`
var eventsSendShort = `Events-Store send messages to an 'events store' channel command`

func NewCmdEventsStoreSend(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &SendOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "send",
		Aliases: []string{"s"},
		Short:   eventsSendShort,
		Long:    eventsSendLong,
		Example: eventsSendExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().StringVarP(&o.metadata, "metadata", "", "", "set body metadata field")
	cmd.PersistentFlags().IntVarP(&o.messages, "messages", "m", 1, "set how many 'events store' messages to send")
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

	fmt.Println("Sending Events Store:")
	for i := 1; i <= o.messages; i++ {
		msg := &pb.Event{
			EventID:  uuid.New(),
			ClientID: client.ClientID(),
			Channel:  o.channel,
			Metadata: o.metadata,
			Body:     []byte(o.body),
			Store:    true,
		}
		result, err := client.SendEvent(ctx, msg)
		if err != nil {
			return fmt.Errorf("sending 'events store' body, %s", err.Error())
		}
		if !result.Sent {
			return fmt.Errorf("sending 'events store' body, %s", result.Error)
		}
		printEventStore(msg)
	}
	return nil
}
