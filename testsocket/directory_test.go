// SPDX-License-Identifier: MIT

package testsocket

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
)

func TestDirectoryUsesDefaultSessionSocketParent(t *testing.T) {
	for _, xdg := range []string{"", t.TempDir()} {
		t.Run(map[bool]string{false: "fallback", true: "xdg"}[xdg != ""], func(t *testing.T) {
			t.Setenv("SESSIONBUS_SOCKET", "")
			t.Setenv("XDG_RUNTIME_DIR", xdg)
			directory := Directory(t)
			parent, err := filepath.EvalSymlinks(filepath.Dir(sessionkit.Socket()))
			if err != nil {
				t.Fatal(err)
			}
			if filepath.Dir(directory) != parent {
				t.Fatalf("test socket parent %q differs from production parent %q", filepath.Dir(directory), filepath.Dir(sessionkit.Socket()))
			}
		})
	}
}

func TestDirectoryResolvesSymlinkBeforeBindingSocket(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	physicalRoot := Directory(t)
	alias := filepath.Join(t.TempDir(), "aliased-runtime-root")
	if err := os.Symlink(physicalRoot, alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_RUNTIME_DIR", alias)
	directory := Directory(t)
	physical, err := filepath.EvalSymlinks(directory)
	if err != nil || directory != physical {
		t.Fatalf("directory %q physical %q: %v", directory, physical, err)
	}
	listener, err := net.Listen("unix", filepath.Join(directory, "bridge.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}
