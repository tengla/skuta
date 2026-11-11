package broker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	natsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/entities"
	"github.com/tengla/skuta/pkg/jstore"
	"github.com/tengla/skuta/pkg/pipe"
	"github.com/tengla/skuta/pkg/storelock"
)

const (
	minMaxPending = 200 * 1024 * 1024
	lockSuffix    = ".lock"
	// clusterUser is the name nodes present to each other with cluster.secret.
	clusterUser = "skuta"
)

func (s *Service) getBrokerOptions(appConfig *config.Config) *natsd.Options {
	opts := &natsd.Options{
		// Clients are all in this process, so nothing listens on a port.
		DontListen:    true,
		NoSigs:        true,
		WriteDeadline: time.Duration(appConfig.Broker.WriteDeadline) * time.Millisecond,
		MaxConn:       appConfig.Broker.MaxConn,
		MaxPayload:    int32(appConfig.Broker.MaxPayload),
		MaxPending:    minMaxPending,

		JetStream:          true,
		StoreDir:           filepath.Join(appConfig.Store.StorePath, appConfig.Host),
		JetStreamMaxStore:  appConfig.Store.MaxStorage,
		JetStreamMaxMemory: appConfig.Store.MaxMemory,
		SyncInterval:       time.Duration(appConfig.Broker.DiskSyncSeconds) * time.Second,
	}
	if int64(opts.MaxPayload) > opts.MaxPending {
		opts.MaxPending = int64(opts.MaxPayload)
	}
	if cluster := appConfig.Cluster; cluster != nil && cluster.Enable {
		// Clients stay in-process; only the other nodes connect, on the cluster
		// port. nats-server doesn't start routing with DontListen, so it gets a
		// client port after all: a random one on loopback, closed to anyone
		// without this process's token.
		opts.DontListen = false
		opts.Host = "127.0.0.1"
		opts.Port = natsd.RANDOM_PORT
		opts.Authorization = nuid.Next() + nuid.Next()
		appConfig.Broker.MemoryPipe.SetToken(opts.Authorization)
		opts.ServerName = appConfig.Host
		opts.Cluster = natsd.ClusterOpts{Name: cluster.Name, Host: "0.0.0.0", Port: cluster.Port}
		opts.Routes = clusterRoutes(cluster.Routes, cluster.Secret)
		if cluster.Secret != "" {
			opts.Cluster.Username = clusterUser
			opts.Cluster.Password = cluster.Secret
		}
	}
	return opts
}

func clusterRoutes(routes []string, secret string) []*url.URL {
	list := make([]*url.URL, 0, len(routes))
	for _, route := range routes {
		u := &url.URL{Scheme: "nats", Host: route}
		if secret != "" {
			u.User = url.UserPassword(clusterUser, secret)
		}
		list = append(list, u)
	}
	return list
}

// waitForCluster waits until the nodes have elected a JetStream leader, which
// takes a majority of them, so stored channels can be used.
func (s *Service) waitForCluster(ctx context.Context) error {
	s.logger.Infow("waiting for the cluster to elect a leader", "routes", s.appConfig.Cluster.Routes)
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	for {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := s.admin.AccountInfo(reqCtx)
		reqCancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the cluster has no leader after %s, are a majority of the nodes in cluster.routes running and reachable: %w", readyTimeout, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// lockStore takes the store's lock, so a second server can't write to the same
// files. The file sits next to the host's directory, not in it, and a volume that
// doesn't support locks only gets a warning, since it worked before there was one.
func (s *Service) lockStore(appConfig *config.Config) error {
	path := filepath.Join(appConfig.Store.StorePath, appConfig.Host+lockSuffix)
	lock, err := storelock.Acquire(path)
	if errors.Is(err, storelock.ErrHeld) {
		return fmt.Errorf("another server is using the store, %s is locked; two servers writing to one store lose messages", path)
	}
	if err != nil {
		s.logger.Warnw("could not lock the store, so nothing stops a second server from using it", "path", path, "error", err)
		return nil
	}
	s.storeLock = lock
	return nil
}

func (s *Service) releaseStore() {
	if s.storeLock != nil {
		s.storeLock.Release()
		s.storeLock = nil
	}
}

// cleanStore removes everything in the store except the lock this server holds.
func cleanStore(appConfig *config.Config) {
	entries, err := os.ReadDir(appConfig.Store.StorePath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.Name() != appConfig.Host+lockSuffix {
			_ = os.RemoveAll(filepath.Join(appConfig.Store.StorePath, entry.Name()))
		}
	}
}

func (s *Service) startAsBroker(ctx context.Context, appConfig *config.Config) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Fatalf("recovering from panic, %v", r)
		}
	}()
	if err := s.lockStore(appConfig); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.releaseStore()
		}
	}()
	if appConfig.Store.CleanStore {
		cleanStore(appConfig)
	}

	s.NatsOptions = s.getBrokerOptions(appConfig)
	s.warnAboutOldStore(s.NatsOptions.StoreDir)
	ns, err := natsd.NewServer(s.NatsOptions)
	if err != nil {
		return err
	}
	ns.SetLogger(s.logger, appConfig.Log.Level == "debug", false)

	go ns.Start()
	if !ns.ReadyForConnections(readyTimeout) {
		ns.Shutdown()
		return entities.ErrServerIsNotReady
	}
	s.Nats = ns
	if appConfig.Store.MaxQueues > 0 {
		// One more for the channel that holds delayed queue messages.
		err := ns.GlobalAccount().UpdateJetStreamLimits(map[string]natsd.JetStreamAccountLimits{
			"": {MaxMemory: -1, MaxStore: -1, MaxConsumers: -1, MaxStreams: appConfig.Store.MaxQueues + 1},
		})
		if err != nil {
			ns.Shutdown()
			return err
		}
	}

	appConfig.Broker.MemoryPipe.Bind(ns, pipe.Limits{
		MaxAgeMinutes:           appConfig.Store.MaxRetention,
		MaxBytes:                appConfig.Store.MaxQueueSize,
		MaxMsgs:                 int64(appConfig.Store.MaxMessages),
		ConsumerInactiveMinutes: appConfig.Store.MaxRetention,
		Replicas:                appConfig.Cluster.StreamReplicas(),
	})
	s.adminConn, err = nats.Connect("", nats.InProcessServer(ns), nats.Name("broker-admin"), nats.Token(s.NatsOptions.Authorization))
	if err != nil {
		ns.Shutdown()
		return err
	}
	s.admin, err = jetstream.New(s.adminConn)
	if err != nil {
		s.adminConn.Close()
		ns.Shutdown()
		return err
	}
	if s.clustered() {
		// The other nodes' subscribers share the queue consumers, so they are
		// not reset here: what a crashed node held comes back after its hold
		// runs out instead.
		if err := s.waitForCluster(ctx); err != nil {
			s.adminConn.Close()
			ns.Shutdown()
			return err
		}
	} else {
		resetCtx, cancel := context.WithTimeout(ctx, readyTimeout)
		defer cancel()
		if err := jstore.ResetQueueConsumers(resetCtx, s.admin); err != nil {
			s.adminConn.Close()
			ns.Shutdown()
			return err
		}
	}
	if inactive := time.Duration(appConfig.Store.MaxPurgeInactive) * time.Minute; inactive > 0 {
		go s.purgeInactiveChannels(ctx, inactive)
	}

	s.isHealthy.Store(true)
	s.isReady.Store(true)
	return nil
}

func (s *Service) clustered() bool {
	return s.appConfig.Cluster != nil && s.appConfig.Cluster.Enable
}

// warnAboutOldStore tells whoever upgraded that NATS Streaming's files aren't
// read anymore. They are left in place.
func (s *Service) warnAboutOldStore(dir string) {
	if _, err := os.Stat(filepath.Join(dir, "server.dat")); err == nil {
		s.logger.Warnw(fmt.Sprintf("found NATS Streaming data in %s; this version stores messages in JetStream and doesn't read it, so messages from before the upgrade are not available", dir))
	}
}
