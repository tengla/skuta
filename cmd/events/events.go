package events

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

var eventsExamples = `
	# Execute send 'events' command
 	skuta events send

	# Execute receive 'events' command
	skuta events receive

	# Execute attach to an 'events' command
	skuta events attach

`
var eventsLong = `Execute Skuta 'events' Pub/Sub commands`
var eventsShort = `Execute Skuta 'events' Pub/Sub commands`

func NewCmdEvents(ctx context.Context, cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:       "events",
		Aliases:   []string{"e", "ev"},
		Short:     eventsShort,
		Long:      eventsLong,
		Example:   eventsExamples,
		ValidArgs: []string{"send", "receive", "attach"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdEventsSend(ctx, cfg))
	cmd.AddCommand(NewCmdEventsReceive(ctx, cfg))
	cmd.AddCommand(NewCmdEventsAttach(ctx, cfg))

	return cmd
}
