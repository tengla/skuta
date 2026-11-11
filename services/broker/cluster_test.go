package broker

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tengla/skuta/config"
	"github.com/tengla/skuta/pkg/jstore"
)

func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	for i := range ports {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		ports[i] = l.Addr().(*net.TCPAddr).Port
	}
	return ports
}

type clusterNode struct {
	cfg  *config.Config
	svc  *Service
	nc   *nats.Conn
	conn *jstore.Conn
}

func (n *clusterNode) stop() {
	if n.svc == nil {
		return
	}
	_ = n.conn.Close()
	n.nc.Close()
	n.svc.Close()
	n.svc = nil
}

// startCluster starts nodes that share nothing but their routes, as pods would.
func startCluster(t *testing.T, size int) []*clusterNode {
	t.Helper()
	ports := freePorts(t, size)
	var routes []string
	for _, port := range ports {
		routes = append(routes, fmt.Sprintf("127.0.0.1:%d", port))
	}
	nodes := make([]*clusterNode, size)
	errs := make(chan error, size)
	for i := range nodes {
		cfg := config.GetDefaultConfig()
		cfg.Host = fmt.Sprintf("node-%d", i)
		cfg.Store.StorePath = t.TempDir()
		cfg.Store.MaxPurgeInactive = 0
		cfg.Cluster.Enable = true
		cfg.Cluster.Port = ports[i]
		cfg.Cluster.Routes = routes
		cfg.Cluster.Secret = "between-nodes"
		if err := cfg.Cluster.Validate(); err != nil {
			t.Fatal(err)
		}
		node := &clusterNode{cfg: cfg}
		nodes[i] = node
		// Together: a node isn't ready before a majority is up.
		go func() {
			svc, err := New(cfg).Start(context.Background())
			if err == nil {
				node.svc = svc
				node.nc, err = nats.Connect("", nats.InProcessServer(cfg.Broker.MemoryPipe), nats.Token(cfg.Broker.MemoryPipe.Token()))
			}
			if err == nil {
				node.conn, err = jstore.Connect(node.nc, cfg.Broker.MemoryPipe, "test-"+cfg.Host)
			}
			errs <- err
		}()
	}
	for range nodes {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.stop()
		}
	})
	return nodes
}

func receive(t *testing.T, ch chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("received %q, want %q", got, want)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

func TestCluster_QueueSurvivesNodeLoss(t *testing.T) {
	nodes := startCluster(t, 3)
	const subject = "_QUEUES_.cluster.q"

	// Sent on one node, received on another.
	if err := nodes[0].conn.Publish(subject, []byte("one")); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 10)
	sub, err := nodes[1].conn.Subscribe(subject, func(m *jstore.Msg) { got <- string(m.Data) }, jstore.QueueDurable(), jstore.DeliverAll())
	if err != nil {
		t.Fatal(err)
	}
	receive(t, got, "one")

	// Every node lists the channel with its subscriber, wherever its leader is.
	for _, node := range nodes {
		queues, err := node.svc.GetQueues(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if queues.TotalQueues != 1 || queues.Queues[0].Name != "cluster.q" || queues.Queues[0].Subscribers != 1 {
			t.Fatalf("%s lists %+v", node.cfg.Host, queues)
		}
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}

	// A message stored before a node is lost is still there after, and the two
	// nodes left keep storing and delivering.
	if err := nodes[0].conn.Publish(subject, []byte("two")); err != nil {
		t.Fatal(err)
	}
	nodes[0].stop()
	if err := nodes[1].conn.Publish(subject, []byte("three")); err != nil {
		t.Fatal(err)
	}
	_, err = nodes[2].conn.Subscribe(subject, func(m *jstore.Msg) { got <- string(m.Data) }, jstore.QueueDurable(), jstore.DeliverAll())
	if err != nil {
		t.Fatal(err)
	}
	receive(t, got, "two")
	receive(t, got, "three")
}

func TestCluster_ConfigValidation(t *testing.T) {
	cfg := config.GetDefaultConfig()
	if err := cfg.Cluster.Validate(); err != nil {
		t.Fatalf("the default, no cluster, must be valid: %v", err)
	}
	if cfg.Cluster.StreamReplicas() != 1 {
		t.Fatalf("one node stores one replica, got %d", cfg.Cluster.StreamReplicas())
	}
	cfg.Cluster.Enable = true
	if err := cfg.Cluster.Validate(); err == nil {
		t.Fatal("a cluster without routes must be refused")
	}
	cfg.Cluster.Routes = []string{"skuta-0.skuta:6222"}
	cfg.Cluster.Replicas = 2
	if err := cfg.Cluster.Validate(); err == nil {
		t.Fatal("two replicas must be refused")
	}
}

func TestCluster_SecretIsRedacted(t *testing.T) {
	cfg := config.GetDefaultConfig()
	cfg.Cluster.Secret = "between-nodes"
	if got := cfg.Redacted().Cluster.Secret; got == "" || got == "between-nodes" {
		t.Fatalf("redacted secret is %q", got)
	}
}
