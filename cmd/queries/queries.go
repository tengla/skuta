package queries

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/utils"
)

var queriesExamples = `
	# Execute send 'queries'  command
	skuta queries send

	# Execute receive 'queries'  command
	skuta queries receive 

	# Execute attach to 'queries' command
	skuta queries attach query

`
var queriesLong = `Execute Skuta 'queries' `
var queriesShort = `Execute Skuta 'queries'`

func NewCmdQueries(ctx context.Context, cfg *config.Config) *cobra.Command {

	cmd := &cobra.Command{
		Use:       "queries",
		Aliases:   []string{"query", "qry"},
		Short:     queriesShort,
		Long:      queriesLong,
		Example:   queriesExamples,
		ValidArgs: []string{"send", "receive", "attach"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdQueriesSend(ctx, cfg))
	cmd.AddCommand(NewCmdQueriesReceive(ctx, cfg))
	cmd.AddCommand(NewCmdQueriesAttach(ctx, cfg))

	return cmd
}
