package events

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/cmd/internal/grpcclient"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

type ReceiveOptions struct {
	cfg     *config.Config
	channel string
	group   string
}

var eventsReceiveExamples = `
	# Receive messages from an 'events' channel (blocks until next body)
	skuta events receive some-channel

	# Receive messages from an 'events' channel with group (blocks until next body)
	skuta events receive some-channel -g G1

`
var eventsReceiveLong = `Events receive (Subscribe) command allows to consume one or many messages from 'events' channel`
var eventsReceiveShort = `Events receive a body from 'events' channel command`

func NewCmdEventsReceive(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ReceiveOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "receive",
		Aliases: []string{"r", "rec", "subscribe", "sub"},
		Short:   eventsReceiveShort,
		Long:    eventsReceiveLong,
		Example: eventsReceiveExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().StringVarP(&o.group, "group", "g", "", "set 'events' channel consumer group (load balancing)")
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
	client, err := grpcclient.New(o.cfg.Client)
	if err != nil {
		return fmt.Errorf("create skuta client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	sub := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Events,
		ClientID:          client.ClientID(),
		Channel:           o.channel,
		Group:             o.group,
	}
	utils.Println("waiting for 'events' messages...")
	err = grpcclient.Receive(ctx, client, func(ctx context.Context) (grpcclient.Stream[*pb.EventReceive], error) {
		return client.SubscribeToEvents(ctx, sub)
	}, func(ev *pb.EventReceive) error {
		printEventReceive(ev)
		return nil
	})
	if err != nil {
		return fmt.Errorf("receive 'events' messages, %s", err.Error())
	}
	return nil
}
