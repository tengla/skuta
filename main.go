package main

import (
	"embed"
	"fmt"
	"github.com/tengla/skuta/cmd/root"
	"github.com/tengla/skuta/services/api"
	"os"
)

var (
	version = ""
)

//go:embed assets/*
var staticAssets embed.FS

func main() {
	api.StaticAssets = staticAssets
	if err := root.Execute(version, os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
