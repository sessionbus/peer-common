// SPDX-License-Identifier: MIT

package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBindTransferPathsJoinsOnlyRelativeLocalPaths(t *testing.T) {
	for _, tc := range []struct{ name, action, args, cwd, want string }{
		{"relative source", "transfer.offer", `{"target":"t","source":"docs/a.txt"}`, "/work", `{"source":"/work/docs/a.txt","target":"t"}`},
		{"absolute source kept", "transfer.offer", `{"target":"t","source":"/etc/a.txt"}`, "/work", `{"target":"t","source":"/etc/a.txt"}`},
		{"relative destination", "transfer.request", `{"target":"t","what":"w","destination":"out.bin","overwrite":true}`, "/work/", `{"destination":"/work/out.bin","overwrite":true,"target":"t","what":"w"}`},
		{"dot-dot kept for the filesystem", "transfer.accept", `{"id":"x","destination":"../up.txt"}`, "/work/sub", `{"destination":"/work/sub/../up.txt","id":"x"}`},
		{"accept source", "transfer.accept", `{"id":"x","source":"a.txt"}`, "/w", `{"id":"x","source":"/w/a.txt"}`},
		{"root cwd", "transfer.offer", `{"target":"t","source":"a"}`, "/", `{"source":"/a","target":"t"}`},
		{"other action untouched", "send", `{"target":"t","message":"source"}`, "", `{"target":"t","message":"source"}`},
		{"status untouched", "transfer.status", `{"id":"x"}`, "", `{"id":"x"}`},
		{"empty path left to the SDK", "transfer.offer", `{"target":"t","source":""}`, "", `{"target":"t","source":""}`},
		{"non-string left to the SDK", "transfer.offer", `{"target":"t","source":7}`, "", `{"target":"t","source":7}`},
		{"invalid JSON left to the SDK", "transfer.offer", `{`, "", `{`},
	} {
		got, err := BindTransferPaths(tc.action, json.RawMessage(tc.args), tc.cwd)
		if err != nil || string(got) != tc.want {
			t.Fatalf("%s: got %s, %v; want %s", tc.name, got, err, tc.want)
		}
	}
}

// Without the thread's working directory a relative path is refused rather
// than resolved against this process's directory.
func TestBindTransferPathsRefusesARelativePathWithoutTheThreadCwd(t *testing.T) {
	for _, cwd := range []string{"", "relative/dir"} {
		if _, err := BindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`), cwd); err == nil ||
			!strings.Contains(err.Error(), "absolute path") {
			t.Fatalf("cwd %q: %v", cwd, err)
		}
	}
	if got, err := BindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"/a.txt"}`), ""); err != nil || string(got) != `{"target":"t","source":"/a.txt"}` {
		t.Fatalf("absolute path without cwd = %s, %v", got, err)
	}
}

func TestBindTransferPathsRefusesAnotherSpellingOfAPathKey(t *testing.T) {
	for _, test := range []struct{ action, args string }{
		{"transfer.offer", `{"target":"t","Source":"a.txt"}`},
		{"transfer.offer", `{"target":"t","ſource":"a.txt"}`},
		{"transfer.offer", `{"target":"t","source":"/abs/a.txt","SOURCE":"a.txt"}`},
		{"transfer.request", `{"target":"t","what":"w","Destination":"a.txt","overwrite":true}`},
		{"transfer.accept", `{"id":"x","Source":"a.txt"}`},
		{"transfer.accept", `{"id":"x","destinatioN":"a.txt"}`},
	} {
		for _, cwd := range []string{"/thread/work", ""} {
			if out, err := BindTransferPaths(test.action, json.RawMessage(test.args), cwd); err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("%s %s (cwd %q) = %s, %v", test.action, test.args, cwd, out, err)
			}
		}
	}
	// The documented spelling is bound as before.
	out, err := BindTransferPaths("transfer.offer", json.RawMessage(`{"target":"t","source":"a.txt"}`), "/thread/work")
	if err != nil || string(out) != `{"source":"/thread/work/a.txt","target":"t"}` {
		t.Fatalf("canonical offer = %s, %v", out, err)
	}
}
