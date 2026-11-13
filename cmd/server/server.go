package server

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/interfaces"
	"github.com/tengla/skuta/pkg/logging"
	"github.com/tengla/skuta/pkg/tracing"
	"github.com/tengla/skuta/pkg/utils"
	"github.com/tengla/skuta/services"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type Options struct {
	cfg *config.Config
}

func NewCmdServer(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &Options{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "server",
		Aliases: []string{"s", "serve"},
		Short:   "Start skuta server",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Run(ctx))
		},
	}

	return cmd
}

func (o *Options) Complete(args []string) error {
	return nil
}

func (o *Options) Validate() error {
	return nil
}

func (o *Options) Run(ctx context.Context) error {
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	logger := logging.GetLogFactory().NewLogger("server")
	logger.Infof("starting Skuta version: %s, cores: %d", o.cfg.GetVersion(), runtime.NumCPU())
	stopTracing, err := tracing.Init(ctx, o.cfg.Tracing, o.cfg.Host, o.cfg.GetVersion())
	if err != nil {
		return err
	}
	srvs, err := services.Start(ctx, o.cfg)
	if err != nil {
		return err
	}
	ifcs, err := interfaces.StartInterfaces(srvs, o.cfg)
	if err != nil {
		return err
	}
	logger.Infof("Skuta web interface is available at http://localhost:%d", o.cfg.Api.Port)
	<-gracefulShutdown
	logger.Warnw("skuta server shutdown started")
	if ifcs != nil {
		ifcs.Close()
	}
	srvs.Close()
	<-srvs.Stopped
	// Flush the spans still waiting to be exported.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	if err := stopTracing(flushCtx); err != nil {
		logger.Warnw("error flushing traces", "error", err.Error())
	}
	cancelFlush()
	logger.Warnw("skuta server shutdown completed")
	logging.Close()
	return nil
}
