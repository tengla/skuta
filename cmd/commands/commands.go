package commands

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

var commandsExamples = `
	# Execute send commands 
	skuta commands send 

	# Execute receive commands
	skuta commands receive 

	# Execute attach to 'commands' channel
	skuta commands attach 
`
var commandsLong = `Execute Skuta 'commands `
var commandsShort = `Execute Skuta 'commands'`

func NewCmdCommands(ctx context.Context, cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:       "commands",
		Aliases:   []string{"cmd", "command"},
		Short:     commandsShort,
		Long:      commandsLong,
		Example:   commandsExamples,
		ValidArgs: []string{"send", "receive", "attach"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdCommandsSend(ctx, cfg))
	cmd.AddCommand(NewCmdCommandsReceive(ctx, cfg))
	cmd.AddCommand(NewCmdCommandsAttach(ctx, cfg))
	return cmd
}
