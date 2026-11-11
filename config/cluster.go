package config

import (
	"net"
	"strings"
)

// ClusterConfig joins this server to others so stored channels are replicated
// and survive the loss of a node. It is off by default: one server needs none
// of it.
//
// Raft needs a majority to agree, so run three nodes (or five). Two nodes stop
// accepting stored messages as soon as either is down. Each node needs its own
// stable HOST, its own store, and the same Name, Routes and Replicas.
type ClusterConfig struct {
	Enable bool `json:"enable"`
	// Name must be the same on every node.
	Name string `json:"name"`
	// Port is where this node listens for the other nodes.
	Port int `json:"port"`
	// Routes are the host:port addresses of the nodes, this one included or not.
	Routes []string `json:"routes"`
	// Replicas is how many nodes hold each queue and events store channel: 1, 3 or 5.
	Replicas int `json:"replicas"`
	// Secret is a password the nodes present to each other. Without one, anything
	// that reaches the cluster port can join and read every message. It must be
	// the same on every node. The connection between nodes is not encrypted.
	Secret string `json:"secret"`
}

func defaultClusterConfig() *ClusterConfig {
	bindViperEnv(
		"Cluster.Enable",
		"Cluster.Name",
		"Cluster.Port",
		"Cluster.Routes",
		"Cluster.Replicas",
		"Cluster.Secret",
	)
	return &ClusterConfig{
		Enable:   false,
		Name:     "skuta",
		Port:     6222,
		Routes:   []string{},
		Replicas: 3,
	}
}

func (c *ClusterConfig) Validate() error {
	if !c.Enable {
		return nil
	}
	if c.Name == "" || strings.ContainsAny(c.Name, " \t.") {
		return NewConfigurationErrorf("bad cluster configuration: name %q must be set, without spaces or dots", c.Name)
	}
	if c.Port <= 0 || c.Port > 65535 {
		return NewConfigurationErrorf("bad cluster configuration: port %d is not a valid port", c.Port)
	}
	if len(c.Routes) == 0 {
		return NewConfigurationError("bad cluster configuration: routes must list the nodes' host:port addresses")
	}
	for _, route := range c.Routes {
		if _, _, err := net.SplitHostPort(route); err != nil {
			return NewConfigurationErrorf("bad cluster configuration: route %q must be host:port", route)
		}
	}
	if strings.ContainsAny(c.Secret, " \t") {
		return NewConfigurationError("bad cluster configuration: secret can't contain spaces")
	}
	if c.Replicas != 1 && c.Replicas != 3 && c.Replicas != 5 {
		return NewConfigurationErrorf("bad cluster configuration: replicas is %d, must be 1, 3 or 5", c.Replicas)
	}
	return nil
}

// StreamReplicas is how many nodes hold each stored channel.
func (c *ClusterConfig) StreamReplicas() int {
	if c == nil || !c.Enable {
		return 1
	}
	return c.Replicas
}
