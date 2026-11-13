package queries

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

var queriesReceiveExamples = `
	# Receive 'queries'  from a 'queries' channel (blocks until next body)
	skuta queries receive some-channel

	# Receive 'queries' from a 'queries' channel with group(blocks until next body)
	skuta queries receive some-channel -g G1
`
var queriesReceiveLong = `Queries receive (Subscribe) command allows to receive a body from a 'queries' channel and response with appropriate reply`
var queriesReceiveShort = `Queries receive a body from a 'queries' channel`

func NewCmdQueriesReceive(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ReceiveOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "receive",
		Aliases: []string{"r", "rec", "subscribe", "sub"},
		Short:   queriesReceiveShort,
		Long:    queriesReceiveLong,
		Example: queriesReceiveExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().StringVarP(&o.group, "group", "g", "", "set 'queries' channel consumer group (load balancing)")
	cmd.PersistentFlags().BoolVarP(&o.autoResponse, "auto-response", "a", false, "set auto response executed query")
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
		SubscribeTypeData: pb.Subscribe_Queries,
		ClientID:          client.ClientID(),
		Channel:           o.channel,
		Group:             o.group,
	}
	utils.Println("waiting for the next query body...")
	err = grpcclient.Receive(ctx, client, func(ctx context.Context) (grpcclient.Stream[*pb.Request], error) {
		return client.SubscribeToRequests(ctx, sub)
	}, func(query *pb.Request) error {
		if err := o.handle(ctx, client, query); err != nil {
			return err
		}
		utils.Println("waiting for the next query body...")
		return nil
	})
	if err != nil {
		return fmt.Errorf("receive queries messages, %s", err.Error())
	}
	return nil
}

func (o *ReceiveOptions) handle(ctx context.Context, client *grpcclient.Client, query *pb.Request) error {
	printQueryReceive(query)
	if o.autoResponse {
		if err := client.Respond(ctx, query, []byte("executed your query"), nil); err != nil {
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
	if !isExecuted {
		return client.Respond(ctx, query, nil, fmt.Errorf("query not executed"))
	}
	respBody := ""
	bodyPrompt := &survey.Input{
		Renderer: survey.Renderer{},
		Message:  "Set response body",
		Default:  "response-to",
		Help:     "",
	}
	if err := survey.AskOne(bodyPrompt, &respBody); err != nil {
		return err
	}
	return client.Respond(ctx, query, []byte(respBody), nil)
}
