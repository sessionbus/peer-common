// SPDX-License-Identifier: MIT
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type privateTestOwner struct {
	*boundsOwner
	request func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error)
}

type shortenedProtocolError struct{ cause *kit.ProtocolError }

func (e shortenedProtocolError) Error() string { return "short" }
func (e shortenedProtocolError) Unwrap() error { return e.cause }

func (o *privateTestOwner) PrivateRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	return o.request(ctx, method, params)
}

type privateFixture struct {
	send    *io.PipeWriter
	receive net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	done    chan struct{}
}

func newPrivateFixture(t *testing.T, owner SessionbusOwner, enabled bool) *privateFixture {
	t.Helper()
	input, send := io.Pipe()
	out, receive := net.Pipe()
	f := &privateFixture{send: send, receive: receive, encoder: json.NewEncoder(send), decoder: json.NewDecoder(receive), done: make(chan struct{})}
	if err := receive.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	go func() { defer close(f.done); _ = serveSessionbus(owner, input, out, ReportHandler{}, enabled) }()
	t.Cleanup(func() { _ = send.Close(); _ = receive.Close(); awaitBounds(t, f.done) })
	return f
}

func (f *privateFixture) sendRequest(t *testing.T, id int, method string, params any) {
	t.Helper()
	if err := f.encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		t.Fatal(err)
	}
}

type privateReply struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *failure        `json:"error"`
}

func (f *privateFixture) read(t *testing.T) privateReply {
	t.Helper()
	var reply privateReply
	if err := f.decoder.Decode(&reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestSessionbusPrivateDispatchIsExplicitAndHidden(t *testing.T) {
	for _, mode := range []string{"absent", "enabled", "inactive"} {
		t.Run(mode, func(t *testing.T) {
			base := newBoundsOwner()
			var public, private atomic.Int32
			base.action = func(context.Context) (json.RawMessage, error) {
				public.Add(1)
				return json.RawMessage(`{"public":true}`), nil
			}
			var owner SessionbusOwner = base
			if mode != "absent" {
				owner = &privateTestOwner{base, func(_ context.Context, method string, _ json.RawMessage) (json.RawMessage, bool, error) {
					private.Add(1)
					if method != "sessionbus/private" {
						t.Errorf("built-in reached private handler: %s", method)
					}
					return json.RawMessage(`{"private":true}`), true, nil
				}}
			}
			f := newPrivateFixture(t, owner, mode != "inactive")
			f.sendRequest(t, 1, "initialize", map[string]string{"protocolVersion": "2024-11-05"})
			initialize := f.read(t)
			var initialized struct {
				ProtocolVersion string
				Capabilities    map[string]json.RawMessage
			}
			if err := json.Unmarshal(initialize.Result, &initialized); err != nil || initialize.Error != nil || initialized.ProtocolVersion != "2024-11-05" {
				t.Fatalf("initialize=%+v err=%v", initialize, err)
			}
			if _, tools := initialized.Capabilities["tools"]; tools != (mode != "inactive") {
				t.Fatal("changed capabilities", initialized)
			}
			f.sendRequest(t, 2, "tools/list", nil)
			listing := f.read(t)
			want, _ := json.Marshal(map[string]any{"tools": []any{Tool()}})
			if mode == "inactive" {
				want = []byte(`{"tools":[]}`)
			}
			if listing.Error != nil || string(listing.Result) != string(want) {
				t.Fatalf("catalog=%s error=%+v", listing.Result, listing.Error)
			}
			f.sendRequest(t, 3, "tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})
			call := f.read(t)
			if mode == "inactive" {
				if call.Error == nil || call.Error.Code != -32602 || public.Load() != 0 {
					t.Fatal("inactive tool changed", call)
				}
			} else if call.Error != nil || public.Load() != 1 {
				t.Fatal("public tool changed", call)
			}
			f.sendRequest(t, 4, "sessionbus/private", nil)
			reply := f.read(t)
			if mode == "enabled" {
				if reply.Error != nil || string(reply.Result) != `{"private":true}` || private.Load() != 1 {
					t.Fatal("missing raw opt-in result", reply, private.Load())
				}
			} else if reply.Error == nil || reply.Error.Code != -32601 || private.Load() != 0 {
				t.Fatal("default dispatch changed", reply, private.Load())
			}
		})
	}
}

func TestSessionbusPrivateResultAndErrorEncoding(t *testing.T) {
	for _, tc := range []struct {
		name, result, want, message string
		handled                     bool
		err                         error
		code                        int
		data                        string
	}{
		{name: "object", result: `{"part":{"id":"prt_native"}}`, want: `{"part":{"id":"prt_native"}}`, handled: true},
		{name: "scalar", result: `7`, want: `7`, handled: true},
		{name: "null", result: `null`, want: `null`, handled: true},
		{name: "nil", want: `{}`, handled: true},
		{name: "protocol", handled: true, err: &kit.ProtocolError{Code: -32004, Message: "wrong generation", Data: json.RawMessage(`{"run":"r1"}`)}, code: -32004, message: "wrong generation", data: `{"run":"r1"}`},
		{name: "ordinary", handled: true, err: errors.New("native request failed"), code: -32603, message: "native request failed"},
		{name: "unhandled", code: -32601, message: "Method not found"},
		{name: "invalid result", handled: true, result: `{`, code: -32603},
		{name: "invalid error data", handled: true, err: &kit.ProtocolError{Code: -32004, Message: "bad data", Data: json.RawMessage(`{`)}, code: -32603},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &privateTestOwner{newBoundsOwner(), func(_ context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
				if method != "sessionbus/private" || string(params) != `{"native":"ses_exact"}` {
					t.Errorf("method=%s params=%s", method, params)
				}
				var result json.RawMessage
				if tc.result != "" {
					result = json.RawMessage(tc.result)
				}
				return result, tc.handled, tc.err
			}}
			f := newPrivateFixture(t, owner, true)
			f.sendRequest(t, 1, "sessionbus/private", map[string]string{"native": "ses_exact"})
			reply := f.read(t)
			if reply.ID != 1 {
				t.Fatal(reply)
			}
			if tc.code == 0 {
				if reply.Error != nil || string(reply.Result) != tc.want {
					t.Fatalf("reply=%+v want=%s", reply, tc.want)
				}
			} else if reply.Error == nil || reply.Result != nil || reply.Error.Code != tc.code || tc.message != "" && reply.Error.Message != tc.message || string(reply.Error.Data) != tc.data {
				t.Fatalf("reply=%+v error=%+v", reply, reply.Error)
			}
			// All errors and malformed callback data leave framing usable.
			f.sendRequest(t, 2, "ping", nil)
			if ping := f.read(t); ping.ID != 2 || ping.Error != nil || string(ping.Result) != `{}` {
				t.Fatal(ping)
			}
		})
	}
}

func TestSessionbusPrivateParamsStayNormalizedObjects(t *testing.T) {
	var calls atomic.Int32
	owner := &privateTestOwner{newBoundsOwner(), func(_ context.Context, _ string, params json.RawMessage) (json.RawMessage, bool, error) {
		calls.Add(1)
		if string(params) != `{}` {
			t.Errorf("params=%s", params)
		}
		return nil, true, nil
	}}
	f := newPrivateFixture(t, owner, true)
	for id, raw := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"sessionbus/private"}`,
		`{"jsonrpc":"2.0","id":2,"method":"sessionbus/private","params":null}`,
		`{"jsonrpc":"2.0","id":3,"method":"sessionbus/private","params":[]}`,
	} {
		if _, err := io.WriteString(f.send, raw+"\n"); err != nil {
			t.Fatal(err)
		}
		reply := f.read(t)
		if reply.ID != id+1 || id < 2 && (reply.Error != nil || string(reply.Result) != `{}`) || id == 2 && (reply.Error == nil || reply.Error.Code != -32602) {
			t.Fatal(reply)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("non-object reached callback", calls.Load())
	}
}

func TestSessionbusPrivateCancellationAndDuplicateIDs(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	releaseCallback := func() { once.Do(func() { close(release) }) }
	defer releaseCallback()
	owner := &privateTestOwner{newBoundsOwner(), func(ctx context.Context, method string, _ json.RawMessage) (json.RawMessage, bool, error) {
		if method == "sessionbus/private" {
			return json.RawMessage(`{"next":true}`), true, nil
		}
		entered <- ctx
		<-ctx.Done()
		<-release
		return nil, true, ctx.Err()
	}}
	f := newPrivateFixture(t, owner, true)
	f.sendRequest(t, 1, "sessionbus/hold", nil)
	var ctx context.Context
	select {
	case ctx = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("private request not admitted")
	}
	f.sendRequest(t, 1, "sessionbus/hold", nil)
	if duplicate := f.read(t); duplicate.Error == nil || duplicate.Error.Code != -32600 {
		t.Fatal("duplicate admitted", duplicate)
	}
	// Public tools and private methods share the existing in-flight ID guard.
	f.sendRequest(t, 1, "tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})
	if duplicate := f.read(t); duplicate.Error == nil || duplicate.Error.Code != -32600 {
		t.Fatal("public call reused private ID", duplicate)
	}
	if err := f.encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]int{"requestId": 1}}); err != nil {
		t.Fatal(err)
	}
	awaitBounds(t, ctx.Done())
	f.sendRequest(t, 2, "ping", nil)
	if ping := f.read(t); ping.ID != 2 || ping.Error != nil {
		t.Fatal("cancelled call replied or reader blocked", ping)
	}
	f.sendRequest(t, 3, "sessionbus/private", nil)
	if next := f.read(t); next.ID != 3 || next.Error != nil || string(next.Result) != `{"next":true}` {
		t.Fatal("subsequent private request failed", next)
	}
	// A cancelled callback remains joined until it actually returns.
	_ = f.send.Close()
	awaitBounds(t, owner.ended)
	select {
	case <-f.done:
		t.Fatal("server returned before callback joined")
	default:
	}
	releaseCallback()
	awaitBounds(t, f.done)
}

func TestSessionbusPrivateCompletionWinsRequestCancellation(t *testing.T) {
	entered := make(chan context.Context, 1)
	owner := &privateTestOwner{newBoundsOwner(), func(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, bool, error) {
		entered <- ctx
		<-ctx.Done()
		return json.RawMessage(`{"admitted":true}`), true, nil
	}}
	f := newPrivateFixture(t, owner, true)
	f.sendRequest(t, 1, "sessionbus/private", nil)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("private request not admitted")
	}
	if err := f.encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]int{"requestId": 1}}); err != nil {
		t.Fatal(err)
	}
	if reply := f.read(t); reply.ID != 1 || reply.Error != nil || string(reply.Result) != `{"admitted":true}` {
		t.Fatal("completion lost to request cancellation", reply)
	}
}

func TestSessionbusPrivateEOFJoinsAndSuppressesLateResult(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	owner := &privateTestOwner{newBoundsOwner(), func(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, bool, error) {
		entered <- ctx
		<-ctx.Done()
		<-release
		return json.RawMessage(`{"tooLate":true}`), true, nil
	}}
	f := newPrivateFixture(t, owner, true)
	// Register after the fixture so an assertion failure releases the callback
	// before fixture cleanup joins the server.
	defer close(release)
	f.sendRequest(t, 1, "sessionbus/private", nil)
	var ctx context.Context
	select {
	case ctx = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("private request not admitted")
	}
	_ = f.send.Close()
	awaitBounds(t, ctx.Done())
	awaitBounds(t, owner.ended)
	select {
	case <-f.done:
		t.Fatal("server returned before callback joined")
	default:
	}
	if _, err := f.receive.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatal("late reply or unclosed output", err)
	}
}

func TestSessionbusPrivateOversizeDataStopsWithoutOutput(t *testing.T) {
	for _, kind := range []string{"result", "ordinary error", "error message", "error data"} {
		t.Run(kind, func(t *testing.T) {
			owner := &privateTestOwner{newBoundsOwner(), func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error) {
				body := strings.Repeat("x", protocol.MaxFrameBytes+1)
				switch kind {
				case "ordinary error":
					return nil, true, errors.New(body)
				case "error data":
					return nil, true, &kit.ProtocolError{Code: -32004, Message: "large", Data: json.RawMessage(`"` + body + `"`)}
				case "error message":
					return nil, true, &kit.ProtocolError{Code: -32004, Message: body}
				default:
					return json.RawMessage(`"` + body + `"`), true, nil
				}
			}}
			f := newPrivateFixture(t, owner, true)
			f.sendRequest(t, 1, "sessionbus/private", nil)
			awaitBounds(t, f.done)
			if _, err := f.receive.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatal("oversized data reached output", err)
			}
		})
	}
}

func TestSessionbusPrivateRetainedResponsesStopAndJoinBlockedWrite(t *testing.T) {
	input, send := io.Pipe()
	out, receive := net.Pipe()
	t.Cleanup(func() { _ = send.Close(); _ = receive.Close() })
	observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
	// Each result is legal alone, including its escaped JSON encoding. Retaining
	// multiple results behind the first blocked response must share the budget.
	body := json.RawMessage(`"` + strings.Repeat("<", protocol.MaxFrameBytes-2) + `"`)
	owner := &privateTestOwner{newBoundsOwner(), func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error) {
		return body, true, nil
	}}
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
	if _, err := send.Write(boundsRequest("sessionbus/private", nil)); err != nil {
		t.Fatal(err)
	}
	awaitBounds(t, observed.entered)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for id := 2; id <= 32; id++ {
			if err := json.NewEncoder(send).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": "sessionbus/private"}); err != nil {
				return
			}
		}
	}()
	awaitBounds(t, owner.ended)
	awaitBounds(t, done)
	awaitBounds(t, sent)
}

func TestSessionbusProtocolErrorBudgetUsesEmittedFields(t *testing.T) {
	for _, path := range []string{"public", "private"} {
		for _, kind := range []string{"message", "data", "combined"} {
			t.Run(path+"/"+kind, func(t *testing.T) {
				inner := &kit.ProtocolError{Code: -32004}
				switch kind {
				case "message":
					inner.Message = strings.Repeat("x", protocol.MaxFrameBytes+1)
				case "data":
					inner.Data = json.RawMessage(`"` + strings.Repeat("x", protocol.MaxFrameBytes) + `"`)
				case "combined":
					inner.Message = strings.Repeat("x", protocol.MaxFrameBytes/2)
					inner.Data = json.RawMessage(`"` + strings.Repeat("x", protocol.MaxFrameBytes/2) + `"`)
				}
				failure := shortenedProtocolError{inner}
				base := newBoundsOwner()
				base.action = func(context.Context) (json.RawMessage, error) { return nil, failure }
				var owner SessionbusOwner = base
				method, params := "tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}}
				if path == "private" {
					method, params = "sessionbus/private", nil
					owner = &privateTestOwner{base, func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error) {
						return nil, true, failure
					}}
				}
				f := newPrivateFixture(t, owner, true)
				f.sendRequest(t, 1, method, params)
				if _, err := f.receive.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Fatal("shortened outer error bypassed returned-data limit", err)
				}
				awaitBounds(t, f.done)
			})
		}
	}
}

func TestSessionbusWrappedProtocolErrorsShareRetainedBudget(t *testing.T) {
	for _, path := range []string{"public", "private"} {
		t.Run(path, func(t *testing.T) {
			input, send := io.Pipe()
			out, receive := net.Pipe()
			t.Cleanup(func() { _ = send.Close(); _ = receive.Close() })
			observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
			failure := shortenedProtocolError{&kit.ProtocolError{Code: -32004, Message: strings.Repeat("x", protocol.MaxFrameBytes-1)}}
			base := newBoundsOwner()
			base.action = func(context.Context) (json.RawMessage, error) { return nil, failure }
			var owner SessionbusOwner = base
			method, params := "tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}}
			if path == "private" {
				method, params = "sessionbus/private", nil
				owner = &privateTestOwner{base, func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error) {
					return nil, true, failure
				}}
			}
			done := make(chan struct{})
			go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
			encoder := json.NewEncoder(send)
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}); err != nil {
				t.Fatal(err)
			}
			awaitBounds(t, observed.entered)
			sent := make(chan struct{})
			go func() {
				defer close(sent)
				for id := 2; id <= 32; id++ {
					if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
						return
					}
				}
			}()
			awaitBounds(t, base.ended)
			awaitBounds(t, done)
			awaitBounds(t, sent)
		})
	}
}
