package events_store

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

var eventsExamples = `
	# Execute send 'events store' command 
	skuta events_store send

	# Execute receive 'events store' command
	skuta events_store receive

	# Execute attach to 'events store' command
	 skuta events_store attach

	# Execute list of 'events store' channels command
 	skuta events_store list
`
var eventsLong = `Execute Skuta 'Events-Store' Pub/Sub commands`
var eventsShort = `Execute Skuta 'Events-Store' Pub/Sub commands`

func NewCmdEventsStore(ctx context.Context, cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:       "events_store",
		Aliases:   []string{"es"},
		Short:     eventsLong,
		Long:      eventsShort,
		Example:   eventsExamples,
		ValidArgs: []string{"send", "receive", "attach", "list"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdEventsStoreSend(ctx, cfg))
	cmd.AddCommand(NewCmdEventsStoreReceive(ctx, cfg))
	cmd.AddCommand(NewCmdEventsStoreAttach(ctx, cfg))
	cmd.AddCommand(NewCmdEventsStoreList(ctx, cfg))

	return cmd
}
