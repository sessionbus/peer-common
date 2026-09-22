// SPDX-License-Identifier: MIT

package peerversion

import (
	"reflect"
	"strings"
	"testing"
)

func TestResolvePublicVersionAndNativeEscape(t *testing.T) {
	release, revision := Release, Revision
	Release, Revision = "v9.8.7", strings.Repeat("a", 40)
	t.Cleanup(func() { Release, Revision = release, revision })

	commands := map[string]string{
		"claude-peer": "-v", "codex-peer": "-V", "grok-peer": "-v", "kilo-peer": "-v",
		"omp-peer": "-v", "opencode-peer": "-v", "pi-peer": "-v", "qwen-peer": "-v",
	}
	for command, short := range commands {
		for _, flag := range []string{short, "--version"} {
			resolved, report, handled := Resolve(command, "/installed/bin/"+command, []string{flag})
			if !handled || resolved != nil || report != command+" v9.8.7 ("+strings.Repeat("a", 40)+")" {
				t.Fatalf("%s %s: resolved=%q report=%q handled=%t", command, flag, resolved, report, handled)
			}
		}
		otherShort := "-V"
		if short == "-V" {
			otherShort = "-v"
		}
		resolved, report, handled := Resolve(command, command, []string{otherShort})
		if handled || report != "" || !reflect.DeepEqual(resolved, []string{otherShort}) {
			t.Fatalf("%s unrelated short flag %s was intercepted", command, otherShort)
		}
		resolved, report, handled = Resolve(command, command, []string{"--native-version"})
		if handled || report != "" || !reflect.DeepEqual(resolved, []string{"--version"}) {
			t.Fatalf("%s native escape: resolved=%q report=%q handled=%t", command, resolved, report, handled)
		}
	}
}

func TestResolveKeepsPrivateAndNonExactArguments(t *testing.T) {
	for _, test := range []struct {
		invoked   string
		arguments []string
	}{
		{"codex-peer-mcp", []string{"--version"}},
		{"sessionbus-hook", []string{"-v"}},
		{"grok-peer", []string{"--version", "extra"}},
		{"qwen-peer", []string{"--native-version", "extra"}},
	} {
		resolved, report, handled := Resolve("codex-peer", test.invoked, test.arguments)
		if handled || report != "" || !reflect.DeepEqual(resolved, test.arguments) {
			t.Fatalf("%s %q: resolved=%q report=%q handled=%t", test.invoked, test.arguments, resolved, report, handled)
		}
	}
}
