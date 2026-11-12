// SPDX-License-Identifier: MIT

package mcp

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestToolDefinitionMatchesSharedArtifact(t *testing.T) {
	want, err := os.ReadFile("testdata/sessionbus-tool.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(Tool())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(got, '\n'), want) {
		t.Fatal("shared Sessionbus tool differs from its authoritative artifact")
	}
}
