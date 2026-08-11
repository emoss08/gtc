// Package ui embeds the built dashboard (a Svelte SPA in ui/dist) into the
// gateway binary, PocketBase-style: the dist directory is committed so plain
// `go build` works without a Node toolchain. Rebuild it with `npm run build`
// in this directory.
package ui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// DistFS returns the built dashboard as a filesystem rooted at dist/, or
// ok=false when no build is present (index.html missing).
func DistFS() (fs.FS, bool) {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil, false
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil, false
	}
	return sub, true
}
