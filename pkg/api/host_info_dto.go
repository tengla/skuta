package api

import (
	"github.com/dustin/go-humanize"
	"math/big"
	"time"
)

type HostInfoDTO struct {
	Host              string  `json:"host"`
	Version           string  `json:"version"`
	LastUpdate        string  `json:"lastUpdate"`
	Status            string  `json:"status"`
	Role              string  `json:"role"`
	Uptime            string  `json:"uptime"`
	MemoryAllocated   string  `json:"memoryAllocated"`
	MemoryUsed        string  `json:"memoryUsed"`
	MemoryUtilization float64 `json:"memoryUtilization"`
	OsThreads         int64   `json:"osThreads"`
	CpuCores          int     `json:"cpuCores"`
	CpuUtilization    float64 `json:"cpuUtilization"`
	ActiveClients     int     `json:"activeClients"`
	// Nodes is how many cluster nodes this one sees, itself included.
	Nodes int `json:"nodes"`
}

func NewHostInfoDTO(system *System) *HostInfoDTO {
	role, nodes := "Standalone", 1
	if system.ClusterNodes > 0 {
		role, nodes = "Cluster", system.ClusterNodes
	}
	return &HostInfoDTO{
		Host:              system.Hostname,
		Version:           system.Version,
		LastUpdate:        time.UnixMilli(system.UpdatedAt).Format("2006-01-02 15:04:05"),
		Status:            "Running",
		Role:              role,
		Nodes:             nodes,
		Uptime:            (time.Duration(system.Uptime) * time.Second).String(),
		MemoryAllocated:   humanize.BigBytes(big.NewInt(int64(system.ProcessMemoryAllocation))),
		MemoryUsed:        humanize.BigBytes(big.NewInt(int64(system.ProcessMemory))),
		MemoryUtilization: system.MemoryUtilization,
		OsThreads:         system.OSThreads,
		CpuCores:          system.TotalCPUs,
		CpuUtilization:    system.CPUUtilization,
		ActiveClients:     system.ActiveClients,
	}
}
