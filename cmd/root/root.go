package root

import (
	"context"
	"github.com/tengla/skuta/cmd/commands"
	"github.com/tengla/skuta/cmd/events"
	"github.com/tengla/skuta/cmd/events_store"
	"github.com/tengla/skuta/cmd/queries"
	"github.com/tengla/skuta/cmd/queue"
	"github.com/tengla/skuta/cmd/server"
	"github.com/tengla/skuta/cmd/token"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/logging"
	"github.com/tengla/skuta/pkg/utils"

	//"github.com/tengla/skuta/pkg/utils"
	"github.com/spf13/cobra"
	//"github.com/spf13/cobra/doc"
)

var rootCmd = &cobra.Command{
	Use: "skuta",
}

func Execute(version string, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.GetAppConfig("./", "./config")
	err := cfg.Validate()
	if err != nil {
		return err
	}
	cfg.SetVersion(version)
	logging.CreateLoggerFactory(ctx, cfg.Host, cfg.Log)
	rootCmd.Version = version
	rootCmd.AddCommand(server.NewCmdServer(ctx, cfg))
	rootCmd.AddCommand(commands.NewCmdCommands(ctx, cfg))
	rootCmd.AddCommand(queries.NewCmdQueries(ctx, cfg))
	rootCmd.AddCommand(events.NewCmdEvents(ctx, cfg))
	rootCmd.AddCommand(events_store.NewCmdEventsStore(ctx, cfg))
	rootCmd.AddCommand(queue.NewCmdQueue(ctx, cfg))
	rootCmd.AddCommand(token.NewCmdToken(ctx, cfg))
	//_ = doc.GenMarkdownTree(rootCmd, "./docs")
	utils.CheckErr(rootCmd.Execute())
	return nil
}
