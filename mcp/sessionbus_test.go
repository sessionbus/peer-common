// SPDX-License-Identifier: MIT
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

type genericOwner struct{ ended bool }

func (*genericOwner) Action(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	panic("no public action expected")
}
func (o *genericOwner) End() { o.ended = true }

func TestSessionbusNativeReportsAreExplicitAndHidden(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_native_handler", true: "native_handler"}[enabled], func(t *testing.T) {
			owner := &genericOwner{}
			called := false
			report := ReportHandler{}
			if enabled {
				report = ReportHandler{Name: "identity", Begin: func(raw json.RawMessage) (<-chan error, error) {
					called = true
					if string(raw) != `{"native":true}` {
						t.Fatalf("raw=%s", raw)
					}
					return nil, nil
				}}
			}
			input, writer := io.Pipe()
			output, reader := io.Pipe()
			done := make(chan error, 1)
			go func() { done <- ServeSessionbus(owner, input, reader, report) }()
			t.Cleanup(func() { _ = writer.Close(); _ = output.Close() })
			encoder, decoder := json.NewEncoder(writer), json.NewDecoder(output)
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}); err != nil {
				t.Fatal(err)
			}
			var listing struct {
				Result struct{ Tools []struct{ Name string } }
			}
			if err := decoder.Decode(&listing); err != nil {
				t.Fatal(err)
			}
			if len(listing.Result.Tools) != 1 || listing.Result.Tools[0].Name != "sessionbus" {
				t.Fatalf("tools=%+v", listing)
			}
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": "identity", "arguments": map[string]bool{"native": true}}}); err != nil {
				t.Fatal(err)
			}
			var reply struct {
				Error  *struct{ Code int }
				Result json.RawMessage
			}
			if err := decoder.Decode(&reply); err != nil {
				t.Fatal(err)
			}
			if enabled && reply.Error != nil || !enabled && (reply.Error == nil || reply.Error.Code != -32602) {
				t.Fatalf("reply=%+v", reply)
			}
			_ = writer.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !owner.ended || called != enabled {
				t.Fatalf("ended=%v called=%v", owner.ended, called)
			}

		})
	}
}

func TestSessionbusReportCannotShadowPublicTool(t *testing.T) {
	for _, name := range []string{"", " ", "sessionbus"} {
		called := false
		var output bytes.Buffer
		err := ServeSessionbus(&genericOwner{}, io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sessionbus","arguments":{}}}`)), &output, ReportHandler{Name: name, Begin: func(json.RawMessage) (<-chan error, error) { called = true; return nil, nil }})
		if err == nil || called || output.Len() != 0 {
			t.Fatalf("name=%q err=%v called=%v output=%s", name, err, called, output.String())
		}
	}
}

type metadataOwner struct {
	genericOwner
	received chan json.RawMessage
}

func (o *metadataOwner) ActionWithMeta(_ context.Context, _ string, _ json.RawMessage, meta json.RawMessage) (json.RawMessage, error) {
	o.received <- append(json.RawMessage(nil), meta...)
	return json.RawMessage(`{}`), nil
}
func TestSessionbusMetadataIsPerNativeRequest(t *testing.T) {
	input, writer := io.Pipe()
	output, reader := io.Pipe()
	owner := &metadataOwner{received: make(chan json.RawMessage, 1)}
	done := make(chan error, 1)
	go func() { done <- ServeSessionbus(owner, input, reader, ReportHandler{}) }()
	encoder, decoder := json.NewEncoder(writer), json.NewDecoder(output)
	for _, id := range []string{"first", "second"} {
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}, "_meta": map[string]string{"threadId": id}}}); err != nil {
			t.Fatal(err)
		}
		if got := string(<-owner.received); got != `{"threadId":"`+id+`"}` {
			t.Fatalf("metadata=%s", got)
		}
		var reply map[string]any
		if err := decoder.Decode(&reply); err != nil {
			t.Fatal(err)
		}
		if reply["id"] != id || reply["error"] != nil {
			t.Fatal(reply)
		}
	}
	writer.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	output.Close()
	reader.Close()
}

func TestInactiveMCPStaysUntilEOFAndAdvertisesNothing(t *testing.T) {
	input, writer := io.Pipe()
	output, reader := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- ServeInactiveSessionbus(input, reader) }()
	encoder, decoder := json.NewEncoder(writer), json.NewDecoder(output)
	calls := []struct {
		method string
		params any
	}{
		{"initialize", map[string]any{"protocolVersion": "2025-06-18"}},
		{"ping", map[string]any{}}, {"tools/list", map[string]any{}},
		{"tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}}},
	}
	for i, call := range calls {
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": i + 1, "method": call.method, "params": call.params}); err != nil {
			t.Fatal(err)
		}
		var frame struct {
			Result map[string]json.RawMessage
			Error  json.RawMessage
		}
		if err := decoder.Decode(&frame); err != nil {
			t.Fatal(err)
		}
		if i < 3 && frame.Error != nil {
			t.Fatalf("%s failed: %s", call.method, frame.Error)
		}
		if i == 0 && string(frame.Result["capabilities"]) != "{}" {
			t.Fatal(frame.Result)
		}
		if i == 2 && string(frame.Result["tools"]) != "[]" {
			t.Fatal(frame.Result)
		}
		if i == 3 && frame.Error == nil {
			t.Fatal("inactive public call succeeded")
		}
		select {
		case err := <-done:
			t.Fatalf("inactive server exited before EOF: %v", err)
		default:
		}
	}
	_ = writer.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	_ = output.Close()
}
