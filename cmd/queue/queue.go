package queue

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

var queueExamples = `
	# Execute send 'queues' command
	skuta queues send

	# Execute attached to 'queues' command
	skuta queues attach

	# Execute receive 'queues' command
	skuta queues receive
	
	# Execute list 'queues' command
	skuta queues list

	# Execute peek 'queues' command
	skuta queues peek

	# Execute ack 'queues' command
	 skuta queues ack

	# Execute stream 'queues' command
	skuta queues stream
`
var queueLong = `Execute Skuta 'queues' commands`
var queueShort = `Execute Skuta 'queues' commands`

func NewCmdQueue(ctx context.Context, cfg *config.Config) *cobra.Command {

	cmd := &cobra.Command{
		Use:       "queues",
		Aliases:   []string{"q", "qu", "queue"},
		Short:     queueShort,
		Long:      queueLong,
		Example:   queueExamples,
		ValidArgs: []string{"send", "receive", "attach", "peek", "ack", "list", "stream"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdQueueSend(ctx, cfg))
	cmd.AddCommand(NewCmdQueueReceive(ctx, cfg))
	cmd.AddCommand(NewCmdQueuePeek(ctx, cfg))
	cmd.AddCommand(NewCmdQueueAck(ctx, cfg))
	cmd.AddCommand(NewCmdQueueList(ctx, cfg))
	cmd.AddCommand(NewCmdQueueStream(ctx, cfg))
	cmd.AddCommand(NewCmdQueueAttach(ctx, cfg))

	return cmd
}
