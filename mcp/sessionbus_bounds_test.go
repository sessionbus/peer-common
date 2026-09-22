// SPDX-License-Identifier: MIT
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type boundsOwner struct {
	action         func(context.Context) (json.RawMessage, error)
	ended          chan struct{}
	initialized    chan struct{}
	once, initOnce sync.Once
}

func (o *boundsOwner) Action(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
	if o.action == nil {
		panic("unexpected action")
	}
	return o.action(ctx)
}
func (o *boundsOwner) End() { o.once.Do(func() { close(o.ended) }) }
func (o *boundsOwner) Initialized() {
	if o.initialized != nil {
		o.initOnce.Do(func() { close(o.initialized) })
	}
}
func newBoundsOwner() *boundsOwner {
	return &boundsOwner{ended: make(chan struct{}), initialized: make(chan struct{})}
}
func awaitBounds(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("boundary did not settle")
	}
}
func boundsRequest(method string, params any) []byte {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	return append(b, '\n')
}

type observedMCPOutput struct {
	io.WriteCloser
	entered chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (w *observedMCPOutput) Write(b []byte) (int, error) {
	w.calls.Add(1)
	w.once.Do(func() { close(w.entered) })
	return w.WriteCloser.Write(b)
}

func TestMCPBlockedResponsesDoNotHoldEOF(t *testing.T) {
	for _, method := range []string{"initialize", "ping", "tools/list", "unknown", "malformed", "tools/call"} {
		for _, enabled := range []bool{false, true} {
			t.Run(method+map[bool]string{false: "/inactive", true: "/enabled"}[enabled], func(t *testing.T) {
				input, send := io.Pipe()
				out, receive := net.Pipe()
				defer send.Close()
				defer receive.Close()
				observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
				owner := newBoundsOwner()
				owner.action = func(context.Context) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }
				done := make(chan struct{})
				go func() { defer close(done); _ = serveSessionbus(owner, input, observed, ReportHandler{}, enabled) }()
				request := boundsRequest(method, map[string]any{"protocolVersion": "2025-06-18", "name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})
				if method == "malformed" {
					request = []byte("{\n")
				}
				if _, err := send.Write(request); err != nil {
					t.Fatal(err)
				}
				awaitBounds(t, observed.entered)
				select {
				case <-owner.initialized:
					t.Fatal("initialize callback preceded completed write")
				default:
				}
				_ = send.Close()
				awaitBounds(t, owner.ended)
				awaitBounds(t, done)
				select {
				case <-owner.initialized:
					t.Fatal("initialize callback survived failed write/EOF")
				default:
				}
				if observed.calls.Load() != 1 {
					t.Fatal("unexpected response writes", observed.calls.Load())
				}
			})
		}
	}
}

func TestMCPBlockedOSPipeWriteJoinsOnEOF(t *testing.T) {
	input, send := io.Pipe()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer send.Close()
	observed := &observedMCPOutput{WriteCloser: write, entered: make(chan struct{})}
	owner := newBoundsOwner()
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
	// Larger than the pipe buffer: no consumer reads the output.
	if _, err = send.Write(boundsRequest("initialize", map[string]string{"protocolVersion": strings.Repeat("v", 1<<20)})); err != nil {
		t.Fatal(err)
	}
	awaitBounds(t, observed.entered)
	_ = send.Close()
	awaitBounds(t, done)
}

func TestMCPInitializedOnlyAfterSuccessfulWrite(t *testing.T) {
	input, send := io.Pipe()
	out, receive := net.Pipe()
	defer send.Close()
	defer receive.Close()
	observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
	owner := newBoundsOwner()
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
	if _, err := send.Write(boundsRequest("initialize", map[string]string{"protocolVersion": "2025-06-18"})); err != nil {
		t.Fatal(err)
	}
	awaitBounds(t, observed.entered)
	select {
	case <-owner.initialized:
		t.Fatal("callback before write completion")
	default:
	}
	var reply any
	if err := json.NewDecoder(receive).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	awaitBounds(t, owner.initialized)
	_ = send.Close()
	awaitBounds(t, done)
}

func TestMCPResponseWorkAndRetainedInputLimits(t *testing.T) {
	for _, size := range []int{256, maxMCPInputFrame} {
		t.Run(map[bool]string{true: "payload", false: "work"}[size == maxMCPInputFrame], func(t *testing.T) {
			input, send := io.Pipe()
			out, receive := net.Pipe()
			defer send.Close()
			defer receive.Close()
			admitted := make(chan struct{}, maxMCPResponseWork)
			never := make(chan error)
			owner := newBoundsOwner()
			done := make(chan struct{})
			report := ReportHandler{Name: "report", Begin: func(json.RawMessage) (<-chan error, error) { admitted <- struct{}{}; return never, nil }}
			go func() { defer close(done); _ = ServeSessionbus(owner, input, out, report) }()
			body := boundsRequest("tools/call", map[string]any{"name": "report", "arguments": map[string]string{"padding": ""}})
			body = bytes.Replace(body, []byte(`"padding":""`), []byte(`"padding":"`+strings.Repeat("x", size-len(body))+`"`), 1)
			if len(body) != size {
				t.Fatal(len(body), size)
			}
			limit := min(maxMCPResponseWork, maxMCPRetainedBytes/size)
			for range limit {
				if _, err := send.Write(body); err != nil {
					t.Fatal(err)
				}
				awaitBounds(t, admitted)
			}
			if _, err := send.Write(body); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatal(err)
			}
			awaitBounds(t, done)
			if len(admitted) != 0 {
				t.Fatal("overflow admitted another callback")
			}
		})
	}
}

func TestMCPReturnedPayloadBudgetStopsBlockedResponses(t *testing.T) {
	input, send := io.Pipe()
	out, receive := net.Pipe()
	defer send.Close()
	defer receive.Close()
	owner := newBoundsOwner()
	raw := json.RawMessage(`"` + strings.Repeat("x", protocol.MaxFrameBytes-2) + `"`)
	owner.action = func(context.Context) (json.RawMessage, error) { return raw, nil }
	observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
	request := func(id int) []byte {
		body := boundsRequest("tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})
		return bytes.Replace(body, []byte(`"id":1`), []byte(`"id":`+jsonNumber(id)), 1)
	}
	if _, err := send.Write(request(1)); err != nil {
		t.Fatal(err)
	}
	// Establish an actual blocked response before charging the remaining
	// results. A burst can legitimately exhaust the budget before any Write.
	awaitBounds(t, observed.entered)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for id := 2; id <= 32; id++ {
			if _, err := send.Write(request(id)); err != nil {
				return
			}
		}
	}()
	awaitBounds(t, done)
	awaitBounds(t, sent)
}
func jsonNumber(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestMCPFrameAndEscapedResultBounds(t *testing.T) {
	for _, newline := range []bool{false, true} {
		input := strings.Repeat("x", maxMCPInputFrame)
		if newline {
			input = input[:len(input)-1] + "\n"
		}
		body, err := readMCPFrame(bufio.NewReader(strings.NewReader(input)))
		if len(body) != maxMCPInputFrame || err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(len(body), err)
		}
	}
	body, err := readMCPFrame(bufio.NewReader(strings.NewReader(strings.Repeat("x", maxMCPInputFrame+1))))
	if err == nil || len(body) != 0 {
		t.Fatal("accepted oversized unterminated frame")
	}
	input, send := io.Pipe()
	out, receive := net.Pipe()
	defer send.Close()
	defer receive.Close()
	owner := newBoundsOwner()
	raw := json.RawMessage(`{"result":"` + strings.Repeat("<", protocol.MaxFrameBytes-100) + `"}`)
	owner.action = func(context.Context) (json.RawMessage, error) { return raw, nil }
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, out, ReportHandler{}) }()
	if _, err := send.Write(boundsRequest("tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})); err != nil {
		t.Fatal(err)
	}
	response, err := bufio.NewReader(receive).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if len(response) < 6*(len(raw)-100) || len(response) > maxMCPResponseFrame {
		t.Fatal("escaped envelope size", len(response))
	}
	var value struct {
		Result struct{ Content []struct{ Text string } }
	}
	if err = json.Unmarshal(response, &value); err != nil {
		t.Fatal(err)
	}
	if len(value.Result.Content) != 1 || value.Result.Content[0].Text != string(raw) {
		t.Fatal("max valid result changed")
	}
	_ = send.Close()
	awaitBounds(t, done)
}

func TestMCPCompletedActionDoesNotWriteAfterEOF(t *testing.T) {
	input, send := io.Pipe()
	out, receive := net.Pipe()
	defer send.Close()
	defer receive.Close()
	observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
	owner := newBoundsOwner()
	entered := make(chan struct{})
	owner.action = func(ctx context.Context) (json.RawMessage, error) {
		close(entered)
		<-ctx.Done()
		return json.RawMessage(`{"done":true}`), nil
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
	if _, err := send.Write(boundsRequest("tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})); err != nil {
		t.Fatal(err)
	}
	awaitBounds(t, entered)
	_ = send.Close()
	awaitBounds(t, done)
	if observed.calls.Load() != 0 {
		t.Fatal("response emitted after EOF")
	}
}

func TestMCPResponseReservationsReleaseAndIDsCanBeReused(t *testing.T) {
	input, send := io.Pipe()
	out, receive := net.Pipe()
	defer send.Close()
	defer receive.Close()
	owner := newBoundsOwner()
	var calls int
	owner.action = func(context.Context) (json.RawMessage, error) {
		calls++
		if calls%2 == 0 {
			return nil, errors.New(strings.Repeat("e", 16<<10))
		}
		return json.RawMessage(`"` + strings.Repeat("x", 128<<10) + `"`), nil
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, out, ReportHandler{}) }()
	decoder := json.NewDecoder(receive)
	// Exceed both cumulative limits, retaining only one call at a time. Reuse
	// the same ID immediately after its reply, including error replies.
	for range maxMCPResponseWork + 1 {
		if _, err := send.Write(boundsRequest("tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})); err != nil {
			t.Fatal(err)
		}
		var reply struct {
			Result json.RawMessage
			Error  json.RawMessage
		}
		if err := decoder.Decode(&reply); err != nil {
			t.Fatal(err)
		}
		if len(reply.Result) == 0 || len(reply.Error) != 0 {
			t.Fatalf("unexpected reply: %+v", reply)
		}
	}
	_ = send.Close()
	awaitBounds(t, done)
}

func TestMCPOversizedPayloadStopsWithoutOutput(t *testing.T) {
	for _, kind := range []string{"input", "encoded", "result"} {
		t.Run(kind, func(t *testing.T) {
			input, send := io.Pipe()
			out, receive := net.Pipe()
			defer send.Close()
			defer receive.Close()
			observed := &observedMCPOutput{WriteCloser: out, entered: make(chan struct{})}
			owner := newBoundsOwner()
			owner.action = func(context.Context) (json.RawMessage, error) {
				return json.RawMessage(strings.Repeat("x", protocol.MaxFrameBytes+1)), nil
			}
			body := boundsRequest("tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})
			if kind == "input" {
				body = []byte(strings.Repeat("x", maxMCPInputFrame+1))
			}
			if kind == "encoded" {
				// Raw '<' is legal JSON input; its echoed version expands sixfold.
				body = []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"` + strings.Repeat("<", (maxMCPResponseFrame/6)+1) + `"}}` + "\n")
			}
			done := make(chan struct{})
			go func() { defer close(done); _ = ServeSessionbus(owner, input, observed, ReportHandler{}) }()
			_, _ = send.Write(body)
			awaitBounds(t, done)
			if observed.calls.Load() != 0 {
				t.Fatal("oversized payload reached writer")
			}
			select {
			case <-owner.initialized:
				t.Fatal("oversized initialize reported ready")
			default:
			}
		})
	}
}

func TestMCPCancelledCallsReleaseWorkWithoutReplies(t *testing.T) {
	input, send := io.Pipe()
	out, receive := net.Pipe()
	defer send.Close()
	defer receive.Close()
	owner := newBoundsOwner()
	entered := make(chan context.Context)
	owner.action = func(ctx context.Context) (json.RawMessage, error) {
		entered <- ctx
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = ServeSessionbus(owner, input, out, ReportHandler{}) }()
	decoder := json.NewDecoder(receive)
	for i := range maxMCPResponseWork + 1 {
		request := boundsRequest("tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list", "arguments": map[string]any{}}})
		request = bytes.Replace(request, []byte(`"id":1`), []byte(`"id":`+jsonNumber(i+1)), 1)
		if _, err := send.Write(request); err != nil {
			t.Fatal(err)
		}
		var callCtx context.Context
		select {
		case callCtx = <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("call not admitted")
		}
		if err := json.NewEncoder(send).Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]int{"requestId": i + 1}}); err != nil {
			t.Fatal(err)
		}
		awaitBounds(t, callCtx.Done())
		ping := bytes.Replace(boundsRequest("ping", map[string]any{}), []byte(`"id":1`), []byte(`"id":0`), 1)
		if _, err := send.Write(ping); err != nil {
			t.Fatal(err)
		}
		var reply struct {
			ID     int
			Result json.RawMessage
		}
		if err := decoder.Decode(&reply); err != nil {
			t.Fatal(err)
		}
		if reply.ID != 0 || string(reply.Result) != "{}" {
			t.Fatalf("cancelled action replied: %+v", reply)
		}
	}
	_ = send.Close()
	awaitBounds(t, done)
}
