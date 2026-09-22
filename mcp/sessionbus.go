// SPDX-License-Identifier: MIT

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"sync"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// SessionbusOwner owns public calls and their connection lifetime.
type SessionbusOwner interface {
	Action(context.Context, string, json.RawMessage) (json.RawMessage, error)
	End()
}

// ReportHandler is an optional native hook capability, never model-advertised.
// Native validation and identity state stay in the product callback.
type ReportHandler struct {
	Name  string
	Begin func(json.RawMessage) (<-chan error, error)
}

func requestID(raw json.RawMessage) (string, bool) {
	var id any
	if json.Unmarshal(raw, &id) != nil {
		return "", false
	}
	switch x := id.(type) {
	case string:
		b, _ := json.Marshal(x)
		return string(b), true
	case float64:
		if math.Trunc(x) == x && math.Abs(x) <= 9007199254740991 {
			b, _ := json.Marshal(x)
			return string(b), true
		}
	}
	return "", false
}

func toolResult(value json.RawMessage, err error) any {
	result := map[string]any{}
	if err != nil {
		var protocolError *kit.ProtocolError
		if errors.As(err, &protocolError) {
			value, _ = json.Marshal(protocolError)
		} else {
			value, _ = json.Marshal(map[string]string{"error": err.Error()})
		}
		result["isError"] = true
	}
	if len(value) == 0 {
		value = json.RawMessage(`{}`)
	}
	result["content"] = []any{map[string]string{"type": "text", "text": string(value)}}
	return result
}

// Serve keeps native reports, cancellation and EOF independent of pending public
// calls. Public request IDs live only until their action settles; responses are
// connection-scoped and are discarded on EOF. Serving owns input and closable
// output; nonclosable blocking output is unsupported.
func ServeSessionbus(owner SessionbusOwner, input io.ReadCloser, output io.Writer, report ReportHandler) error {
	return serveSessionbus(owner, input, output, report, true)
}

// ServeInactiveSessionbus speaks ordinary MCP without activating integration.
// No product identity, bus connection, hidden handler or public tool is created.
func ServeInactiveSessionbus(input io.ReadCloser, output io.Writer) error {
	return serveSessionbus(inactiveOwner{}, input, output, ReportHandler{}, false)
}

type inactiveOwner struct{}

func (inactiveOwner) Action(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("Sessionbus integration is inactive")
}
func (inactiveOwner) End() {}

// These bound payload bytes, not Go object overhead. Nothing is preallocated
// to these byte limits. Encoding is serialized, with one additional response
// buffer: raw '<' in a valid bus result expands sixfold in MCP's JSON string.
const (
	maxMCPInputFrame    = 2 * protocol.MaxFrameBytes
	maxMCPResponseFrame = 8 * protocol.MaxFrameBytes
	maxMCPResponseWork  = 256
	maxMCPRetainedBytes = 32 * protocol.MaxFrameBytes
)

func readMCPFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		if len(frame) == maxMCPInputFrame {
			if _, err := reader.Peek(1); err != nil {
				return frame, err
			}
			return nil, errors.New("MCP input frame exceeds limit")
		}
		part, err := reader.ReadSlice('\n')
		if len(part) > maxMCPInputFrame-len(frame) {
			return nil, errors.New("MCP input frame exceeds limit")
		}
		frame = append(frame, part...)
		if !errors.Is(err, bufio.ErrBufferFull) {
			return frame, err
		}
	}
}

// Serving owns input and any closable output. Closing real pipes/sockets
// interrupts blocked writes; arbitrary nonclosable blocking Writers are not
// supported. EOF tears down ownership, without a queued-response drain promise.
func serveSessionbus(owner SessionbusOwner, input io.ReadCloser, output io.Writer, report ReportHandler, enabled bool) error {
	if report.Begin != nil && (strings.TrimSpace(report.Name) == "" || report.Name == "sessionbus") {
		return errors.New("hidden report name must be nonempty and distinct from sessionbus")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var state, writes sync.Mutex
	var workers sync.WaitGroup
	pending := map[string]context.CancelFunc{}
	var workCount, retained int
	var once sync.Once
	stop := func() {
		once.Do(func() {
			state.Lock()
			cancel()
			state.Unlock()
			_ = input.Close()
			if closer, ok := output.(io.Closer); ok {
				_ = closer.Close()
			}
			owner.End()
		})
	}
	defer stop()
	reserve := func(size int) *int {
		state.Lock()
		if ctx.Err() != nil {
			state.Unlock()
			return nil
		}
		if workCount == maxMCPResponseWork || size > maxMCPRetainedBytes-retained {
			state.Unlock()
			stop()
			return nil
		}
		workCount++
		retained += size
		workers.Add(1)
		state.Unlock()
		return &size
	}
	charge := func(work *int, size int) bool {
		state.Lock()
		if ctx.Err() != nil {
			state.Unlock()
			return false
		}
		if size > maxMCPRetainedBytes-retained {
			state.Unlock()
			stop()
			return false
		}
		retained += size
		*work += size
		state.Unlock()
		return true
	}
	launch := func(work *int, fn func()) {
		go func() {
			defer workers.Done()
			defer func() { state.Lock(); workCount--; retained -= *work; state.Unlock() }()
			fn()
		}()
	}
	write := func(frame any) bool {
		writes.Lock()
		if ctx.Err() != nil {
			writes.Unlock()
			return false
		}
		body, err := json.Marshal(frame)
		if err == nil && len(body)+1 > maxMCPResponseFrame {
			err = errors.New("MCP response frame exceeds limit")
		}
		if err == nil {
			body = append(body, '\n')
			var n int
			n, err = output.Write(body)
			if err == nil && n != len(body) {
				err = io.ErrShortWrite
			}
		}
		writes.Unlock()
		if err != nil {
			stop()
			return false
		}
		return true
	}
	failure := func(id json.RawMessage, code int, message string) {
		if len(id) == 0 {
			id = json.RawMessage(`null`)
		}
		write(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
	}
	respond := func(id json.RawMessage, value any) bool {
		return write(map[string]any{"jsonrpc": "2.0", "id": id, "result": value})
	}
	failAsync := func(id json.RawMessage, code int, message string, size int) {
		if work := reserve(size); work != nil {
			launch(work, func() { failure(id, code, message) })
		}
	}
	toolReply := func(work *int, id json.RawMessage, value json.RawMessage, err error) {
		// Reserve returned data before it can wait behind another response write.
		// Conservative double charging covers the raw result and its text copy;
		// error JSON can escape each source byte sixfold before the text copy.
		size := len(value)
		if err != nil {
			size = len(err.Error())
			var rpcError *kit.ProtocolError
			if errors.As(err, &rpcError) {
				size += len(rpcError.Data)
			}
			if size > protocol.MaxFrameBytes {
				stop()
				return
			}
			size = 6*size + 128
		} else if size > protocol.MaxFrameBytes {
			stop()
			return
		}
		if !charge(work, 2*size) {
			return
		}
		respond(id, toolResult(value, err))
	}
	reader := bufio.NewReader(input)
	for ctx.Err() == nil {
		line, readErr := readMCPFrame(reader)
		if len(line) == 0 {
			break
		}
		var frame map[string]json.RawMessage
		if !json.Valid(line) {
			failAsync(nil, -32700, "Parse error", len(line))
			if readErr != nil {
				break
			}
			continue
		}
		if json.Unmarshal(line, &frame) != nil || frame == nil {
			failAsync(nil, -32600, "Invalid Request", len(line))
			continue
		}
		id, hasID := frame["id"]
		key, validID := requestID(id)
		var version, method string
		if json.Unmarshal(frame["jsonrpc"], &version) != nil || version != "2.0" || json.Unmarshal(frame["method"], &method) != nil || string(frame["method"]) == "null" || hasID && !validID {
			if !validID {
				id = nil
			}
			failAsync(id, -32600, "Invalid Request", len(line))
			continue
		}
		var params map[string]json.RawMessage
		p := frame["params"]
		if p == nil || string(p) == "null" {
			p = json.RawMessage(`{}`)
		}
		paramsErr := json.Unmarshal(p, &params)
		if !hasID {
			if method == "notifications/cancelled" && paramsErr == nil {
				target, valid := requestID(params["requestId"])
				if valid {
					state.Lock()
					if abort := pending[target]; abort != nil {
						abort()
					}
					state.Unlock()
				}
			}
			continue
		}
		work := reserve(len(line))
		if work == nil {
			break
		}
		if paramsErr != nil || params == nil {
			launch(work, func() { failure(id, -32602, "Invalid parameters") })
			continue
		}
		switch method {
		case "initialize":
			var version string
			if string(params["protocolVersion"]) == "null" || json.Unmarshal(params["protocolVersion"], &version) != nil {
				launch(work, func() { failure(id, -32602, "Invalid initialize parameters") })
				continue
			}
			launch(work, func() {
				capabilities := map[string]any{}
				if enabled {
					capabilities["tools"] = map[string]any{}
				}
				if respond(id, map[string]any{"protocolVersion": version, "capabilities": capabilities, "serverInfo": map[string]string{"name": "sessionbus", "version": "0.5.0"}}) {
					// Serialize callback admission with stop, not with physical writes.
					state.Lock()
					if ready, ok := owner.(interface{ Initialized() }); ok && ctx.Err() == nil {
						ready.Initialized()
					}
					state.Unlock()
				}
			})
		case "ping":
			launch(work, func() { respond(id, map[string]any{}) })
		case "tools/list":
			launch(work, func() {
				tools := []any{}
				if enabled {
					tools = append(tools, Tool())
				}
				respond(id, map[string]any{"tools": tools})
			})
		case "tools/call":
			if !enabled {
				launch(work, func() { failure(id, -32602, "Sessionbus integration is inactive") })
				continue
			}
			var name string
			if json.Unmarshal(params["name"], &name) != nil || name != "sessionbus" && (report.Begin == nil || name != report.Name) {
				launch(work, func() { failure(id, -32602, "Unknown tool") })
				continue
			}
			state.Lock()
			_, duplicate := pending[key]
			state.Unlock()
			if duplicate {
				launch(work, func() { failure(id, -32600, "Request ID already in flight") })
				continue
			}
			if report.Begin != nil && name == report.Name {
				// Admission stays reader-ordered; only completion and output may wait.
				done, err := report.Begin(params["arguments"])
				launch(work, func() {
					if err == nil && done != nil {
						select {
						case err = <-done:
						case <-ctx.Done():
							return
						}
					}
					toolReply(work, id, nil, err)
				})
			} else {
				callCtx, abort := context.WithCancel(ctx)
				state.Lock()
				pending[key] = abort
				state.Unlock()
				launch(work, func() {
					defer abort()
					actionOwner := sessionbusCallOwner{SessionbusOwner: owner, meta: params["_meta"]}
					value, err := CallTool(callCtx, actionOwner, params["arguments"])
					state.Lock()
					delete(pending, key)
					state.Unlock()
					// Completion wins request cancellation while the transport lives.
					if err != nil && callCtx.Err() != nil && errors.Is(err, callCtx.Err()) {
						return
					}
					toolReply(work, id, value, err)
				})
			}
		default:
			launch(work, func() { failure(id, -32601, "Method not found") })
		}
		if readErr != nil {
			break
		}
	}
	stop()
	workers.Wait()
	return nil
}

// Native metadata is dispatched per request, never stored as connection-global
// identity: concurrent tool calls can belong to different native threads.
type sessionbusCallOwner struct {
	SessionbusOwner
	meta json.RawMessage
}

func (o sessionbusCallOwner) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	if native, ok := o.SessionbusOwner.(interface {
		ActionWithMeta(context.Context, string, json.RawMessage, json.RawMessage) (json.RawMessage, error)
	}); ok {
		return native.ActionWithMeta(ctx, action, args, o.meta)
	}
	return o.SessionbusOwner.Action(ctx, action, args)
}
