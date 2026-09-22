// SPDX-License-Identifier: MIT

package peer_common_test

import (
	"bytes"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSharedRepositoryBoundary(t *testing.T) {
	allowed := map[string]bool{".git": true, ".github": true, ".forgejo": true, ".gitignore": true, ".golangci.yml": true, "LICENSE": true, "README.md": true, "architecture_test.go": true, "docs": true, "go.mod": true, "go.sum": true, "host": true, "mcp": true, "peerversion": true, "testsocket": true, "scripts": true}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !allowed[e.Name()] {
			t.Errorf("outside shared-support boundary: %s", e.Name())
		}
	}
	for _, name := range []string{"cmd", "wrappers", "codex", "claude", "grok", "qwen", "pi", "omp", "opencode", "kilo", "go.work", "go.work.sum"} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Errorf("product or workspace path remains: %s", name)
		}
	}
}

func TestPublicSDKAndIndependentModuleBoundary(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"module github.com/sessionbus/peer-common\n", "github.com/antst/sessionbus/bus/sdk/go v0.5.7"} {
		if !bytes.Contains(data, []byte(want)) {
			t.Fatalf("module missing %q", want)
		}
	}
	if bytes.Contains(data, []byte("replace ")) {
		t.Fatal("shared module must not depend on local replacements")
	}
	err = filepath.WalkDir(".", func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			if e.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			name, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(name, "github.com/antst/sessionbus/") && name != "github.com/antst/sessionbus/bus/sdk/go" && !strings.HasPrefix(name, "github.com/antst/sessionbus/bus/sdk/go/") {
				t.Errorf("%s imports non-public Bus code %s", path, name)
			}
			if strings.HasPrefix(name, "github.com/antst/sessionbus-peers") || strings.HasPrefix(name, "github.com/sessionbus/codex-peer") {
				t.Errorf("%s imports product repository %s", path, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
