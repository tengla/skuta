package commands

import (
	"context"
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/cmd/internal/grpcclient"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

type ReceiveOptions struct {
	cfg          *config.Config
	channel      string
	group        string
	autoResponse bool
}

var commandsReceiveExamples = `
	# Receive commands from a 'commands' channel (blocks until next body)
	skuta commands receive some-channel

	# Receive commands from a 'commands' channel with group (blocks until next body)
	skuta commands receive some-channel -g G1
`
var commandsReceiveLong = `Command receive (Subscribe) command allows to consume a body from 'commands' channel and response with appropriate reply`
var commandsReceiveShort = `Command receive a body from 'commands' channel command`

func NewCmdCommandsReceive(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ReceiveOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "receive",
		Aliases: []string{"r", "rec", "subscribe", "sub"},
		Short:   commandsReceiveShort,
		Long:    commandsReceiveLong,
		Example: commandsReceiveExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().StringVarP(&o.group, "group", "g", "", "set 'commands' channel consumer group (load balancing)")
	cmd.PersistentFlags().BoolVarP(&o.autoResponse, "auto-response", "a", false, "set auto response executed command for each command received")
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
		SubscribeTypeData: pb.Subscribe_Commands,
		ClientID:          client.ClientID(),
		Channel:           o.channel,
		Group:             o.group,
	}
	utils.Println("waiting for the next command body...")
	err = grpcclient.Receive(ctx, client, func(ctx context.Context) (grpcclient.Stream[*pb.Request], error) {
		return client.SubscribeToRequests(ctx, sub)
	}, func(command *pb.Request) error {
		if err := o.handle(ctx, client, command); err != nil {
			return err
		}
		utils.Println("waiting for the next command body...")
		return nil
	})
	if err != nil {
		return fmt.Errorf("receive commands messages, %s", err.Error())
	}
	return nil
}

func (o *ReceiveOptions) handle(ctx context.Context, client *grpcclient.Client, command *pb.Request) error {
	printCommandReceive(command)
	if o.autoResponse {
		if err := client.Respond(ctx, command, nil, nil); err != nil {
			return err
		}
		utils.Println("auto execution sent executed response ")
		return nil
	}
	var isExecuted bool
	prompt := &survey.Confirm{
		Renderer: survey.Renderer{},
		Message:  "Set executed ?",
		Help:     "",
	}
	if err := survey.AskOne(prompt, &isExecuted); err != nil {
		return err
	}
	if isExecuted {
		return client.Respond(ctx, command, nil, nil)
	}
	return client.Respond(ctx, command, nil, fmt.Errorf("command not executed"))
}
