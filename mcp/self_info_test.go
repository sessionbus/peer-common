// SPDX-License-Identifier: MIT

package mcp

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

type selfInfoOwner struct {
	*kit.Caller
}

func (*selfInfoOwner) End() {}

func TestListSelfInfoSurvivesCallerAndNativeToolResponse(t *testing.T) {
	// No matching self row: the model must receive daemon-bound self_info,
	// even when a remote host filter excludes the originating caller.
	want := json.RawMessage(`{"sessions":[],"self_info":{"session_id":"native@alpha","name":"Same title@alpha","product":"fixture","groups":["team"]}}`)
	owner := &selfInfoOwner{Caller: kit.NewCaller(func(_ context.Context, method string, raw any) (json.RawMessage, error) {
		if method != "session.list" {
			t.Errorf("method = %q", method)
		}
		return want, nil
	})}
	input, writer := io.Pipe()
	output, reader := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- ServeSessionbus(owner, input, reader, ReportHandler{}) }()
	t.Cleanup(func() { _ = writer.Close(); _ = output.Close(); <-done })
	request := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sessionbus","arguments":{"action":"list","arguments":{"host":"beta"}}}}`)
	if err := json.NewEncoder(writer).Encode(request); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Error  json.RawMessage
		Result struct {
			IsError bool
			Content []struct{ Text string }
		}
	}
	if err := json.NewDecoder(output).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	if len(reply.Error) != 0 || reply.Result.IsError || len(reply.Result.Content) != 1 {
		t.Fatalf("MCP response = %+v", reply)
	}
	var gotValue, wantValue any
	if err := json.Unmarshal([]byte(reply.Result.Content[0].Text), &gotValue); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("result = %#v, want %#v", gotValue, wantValue)
	}
}
