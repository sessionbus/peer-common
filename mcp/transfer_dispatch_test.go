// SPDX-License-Identifier: MIT

package mcp

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"testing"
)

type recordingOwner struct {
	mu    sync.Mutex
	calls [][2]string
}

func (o *recordingOwner) Action(_ context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, [2]string{action, string(args)})
	return json.RawMessage(`{}`), nil
}
func (*recordingOwner) End() {}

// The served tool hands each transfer action's arguments, including the local
// source, destination and overwrite fields the SDK consumes, to the owner
// byte for byte.
func TestServedTransferActionsReachTheOwnerVerbatim(t *testing.T) {
	owner := &recordingOwner{}
	input, writer := io.Pipe()
	output, reader := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- ServeSessionbus(owner, input, reader, ReportHandler{}) }()
	t.Cleanup(func() { _ = writer.Close(); _ = output.Close(); <-done })
	sent := [][2]string{
		{"transfer.offer", `{"target":"peer@local","source":"docs/a.txt","name":"a.txt"}`},
		{"transfer.accept", `{"id":"tr-1","destination":"out/b.bin","overwrite":true}`},
		{"transfer.accept", `{"id":"tr-2","source":"c.txt"}`},
	}
	decoder := json.NewDecoder(output)
	for i, call := range sent {
		request := `{"jsonrpc":"2.0","id":` + strconv.Itoa(i+1) + `,"method":"tools/call","params":{"name":"sessionbus","arguments":{"action":"` + call[0] + `","arguments":` + call[1] + `}}}` + "\n"
		if _, err := io.WriteString(writer, request); err != nil {
			t.Fatal(err)
		}
		var reply struct {
			Error  json.RawMessage
			Result struct{ IsError bool }
		}
		if err := decoder.Decode(&reply); err != nil || len(reply.Error) != 0 || reply.Result.IsError {
			t.Fatalf("%s reply = %+v, %v", call[0], reply, err)
		}
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.calls) != len(sent) {
		t.Fatalf("owner calls = %q", owner.calls)
	}
	for i, call := range sent {
		if owner.calls[i] != call {
			t.Fatalf("owner call %d = %q, want %q", i, owner.calls[i], call)
		}
	}
}
