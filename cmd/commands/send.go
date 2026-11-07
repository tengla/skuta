package commands

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/cmd/internal/grpcclient"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
	"github.com/tengla/skuta/pkg/uuid"
	"time"
)

type SendOptions struct {
	cfg      *config.Config
	channel  string
	body     string
	metadata string
	timeout  int
}

var commandsSendExamples = `
	# Send command to a 'commands' channel
	kubemq commands send some-channel some-command
	
	# Send command to a 'commands' channel with metadata
	kubemq commands send some-channel some-body -m some-metadata
	
	# Send command to a 'commands' channel with 120 seconds timeout
	kubemq commands send some-channel some-body -o 120
`
var commandsSendLong = `Command send command allow to send messages to 'commands' channel with an option to set command time-out`
var commandsSendShort = `Command send messages to 'commands' channel command`

func NewCmdCommandsSend(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &SendOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "send",
		Aliases: []string{"s"},
		Short:   commandsSendShort,
		Long:    commandsSendLong,
		Example: commandsSendExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().StringVarP(&o.metadata, "metadata", "m", "", "Set metadata body")
	cmd.PersistentFlags().IntVarP(&o.timeout, "timeout", "o", 30, "Set command timeout")
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
		return fmt.Errorf("create kubemq client, %s", err.Error())
	}
	defer func() {
		_ = client.Close()
	}()
	msg := &pb.Request{
		RequestID:       uuid.New(),
		RequestTypeData: pb.Request_Command,
		ClientID:        client.ClientID(),
		Channel:         o.channel,
		Metadata:        o.metadata,
		Body:            []byte(o.body),
		Timeout:         int32((time.Duration(o.timeout) * time.Second).Milliseconds()),
	}
	fmt.Println("Sending Command:")
	printCommand(msg)
	res, err := client.SendRequest(ctx, msg)
	if err != nil {
		return fmt.Errorf("sending commands body, %s", err.Error())
	}
	fmt.Println("Getting Command Response:")
	printCommandResponse(res)
	return nil
}
