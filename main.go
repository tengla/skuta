package main

import (
	"embed"
	"fmt"
	"github.com/tengla/skuta/cmd/root"
	"github.com/tengla/skuta/services/api"
	"os"
	"runtime/debug"
)

// version is set at build time with -ldflags "-X main.version=...". The
// release version is the git tag; Taskfile.yml derives it with git describe.
var (
	version = ""
)

//go:embed assets/*
var staticAssets embed.FS

func main() {
	api.StaticAssets = staticAssets
	if err := root.Execute(buildVersion(), os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// buildVersion returns the version injected with -ldflags, or for a plain
// `go build` the commit Go stamped into the binary, e.g. "devel-4356179-dirty".
// The module path has no /v2 suffix, so Go's own module version can't
// reflect the v2 tags and isn't used.
func buildVersion() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	var revision string
	var modified bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if revision == "" {
		return "devel"
	}
	if len(revision) > 7 {
		revision = revision[:7]
	}
	if modified {
		return "devel-" + revision + "-dirty"
	}
	return "devel-" + revision
}
