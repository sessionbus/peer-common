// SPDX-License-Identifier: MIT

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	kit "github.com/antst/sessionbus/bus/sdk/go"
	"slices"
)

const toolDescription = `Call Sessionbus using the current Sessionbus identity. Written means local write completion only.
Arguments by action (wire validation remains in the public kit):
list: {} or {session_id:string} or {host:string}; session_id and host are exclusive. Returns self_info {session_id,name?,product,groups} for the bound originating caller, independent of filters or queried host. Compare row IDs to self_info.session_id; never infer self from names or order. Older daemons may omit self_info.
send: {message:nonempty string, target:string} or {message, targets:unique nonempty string[]} or {message, group:string, host?:string}; choose exactly one addressing form; host applies only to group. Use returned session IDs or unambiguous names. send accepts only message, target, targets, group and host; there is no summary field. Put all intended content in message. Example: {"action":"send","arguments":{"target":"RETURNED_SESSION_ID","message":"Your complete message"}}.
describe: {product:string, host?:string}; returns supported open fields and native extra arguments.
spawn fresh: {product:string, name:string, open:object, host?:string, extra_groups?:unique string[], persistent?:boolean, auto_close_ms?:nonnegative integer, notify?:boolean, notify_target?:string, trace?:"off"|"events"|"content"}. open accepts cwd, permission_mode, model, reasoning_effort (strings), arguments (string[]); use describe to check product support. spawn resume: {resume_session_id:string, persistent?:boolean, auto_close_ms?:nonnegative integer, notify?:boolean, notify_target?:string, trace?:"off"|"events"|"content"}. Choose a product using describe (for example "claude-peer" or "codex-peer"); use returned session_id, never a shell substitute.
trace: {session_id:string, mode:"off"|"events"|"content"}; changes live tracing for a direct child and returns its effective mode. Tracing defaults off, applies only to subsequent Sessionbus message sends and settled delivery metadata, and is not retained. events omits message bodies; content includes them. Trace copies are daemon-generated JSON trace envelopes delivered as ordinary messages. Each starts or schedules native work under the same mandatory-wake contract and is not replayed. It does not include Run or lane lifecycle events.
Policies are independent: persistent=false ends the lane when its owner leaves; true survives owner exit. auto_close_ms defaults to 60000 after a native completed/failed/interrupted terminal; 0 disables. An unavailable record without a native terminal does not start a new grace. Every lane message starts or schedules native work; there is no passive idle mode. Open itself starts no work or retirement deadline. Resume preserves persistence, while omitted auto_close_ms resets to 60000; persistence can be promoted, not demoted. Parent-owned lanes notify their owner unless notify=false; persistent lanes need an explicit notify_target (or one retained on resume/promotion). queued_for_next_turn means the delivery is retained in a bounded in-memory queue for an automatic next native run; it is neither durable storage nor model consumption. A completion pointer is an ordinary peer message from the lane, not the answer. Interactive wake follows its native carrier; delivery success does not prove collection.
run: {session_id:string, input:nonempty string}; starts and waits, returning session_id/run_id/state and either result (done) or reason (unavailable), without consuming the record.
start: same fields as run; returns {session_id,run_id} for collection by an authorized caller while that worker lives.
status: {session_id:string, run_id?:string}; non-consuming read, omitted run_id selects oldest unacknowledged record.
wait: {session_id:string, run_id?:string, timeout_ms?:nonnegative integer}; non-consuming read or running at the requested bound. Cancellation stops this wait, not the native turn or result retention.
ack: {session_id:string, run_id:string}; acknowledge the oldest terminal record: for state=done, first receive and use its result/outcome/native reason; for state=unavailable, first record/report its reason (there is no result). Both states must be acknowledged to advance the cursor. Never acknowledge running or infer an acknowledgeable record from an RPC error. Repeating an acknowledgment is idempotent, without returning the answer again. No skipping older records; unavailable does not claim a native terminal. Closing, auto-close or worker loss makes unacknowledged output unavailable; collection does not reset the retirement deadline.
interrupt: {session_id:string}; acknowledgment is not terminal completion.
close: {session_id:string, forget?:boolean}; forget: {session_id:string} closes with forget=true.
No unlisted argument fields. Message and input strings have a 262144-character wire limit. Native policy can deny any public call.`

func Tool() any {
	return map[string]any{"name": "sessionbus", "description": toolDescription, "inputSchema": map[string]any{
		"type": "object", "required": []string{"action", "arguments"}, "additionalProperties": false,
		"properties": map[string]any{"action": map[string]any{"type": "string", "enum": kit.Actions}, "arguments": argumentSchema()},
	}}
}

// Keep the model-facing schema simple for native MCP consumers. This is the
// closed union of action fields; the public kit still validates each action's
// required fields, addressing alternatives, limits, and policy combinations.
func argumentSchema() map[string]any {
	properties := map[string]any{}
	for _, field := range []string{"session_id", "host", "message", "target", "group", "product", "name", "resume_session_id", "notify_target", "input", "run_id"} {
		properties[field] = map[string]any{"type": "string"}
	}
	for _, field := range []string{"targets", "extra_groups"} {
		properties[field] = map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
	}
	for _, field := range []string{"persistent", "notify", "forget"} {
		properties[field] = map[string]any{"type": "boolean"}
	}
	for _, field := range []string{"auto_close_ms", "timeout_ms"} {
		properties[field] = map[string]any{"type": "integer"}
	}
	properties["trace"] = map[string]any{"type": "string", "enum": []string{"off", "events", "content"}}
	properties["mode"] = map[string]any{"type": "string", "enum": []string{"off", "events", "content"}}
	open := map[string]any{}
	for _, field := range []string{"cwd", "permission_mode", "model", "reasoning_effort"} {
		open[field] = map[string]any{"type": "string"}
	}
	open["arguments"] = map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
	properties["open"] = map[string]any{"type": "object", "additionalProperties": false, "properties": open}
	return map[string]any{"type": "object", "additionalProperties": false, "properties": properties,
		"description": "Use only the fields listed for the selected action in the tool description. send has no summary field; put the complete content in message."}
}

func CallTool(ctx context.Context, owner interface {
	Action(context.Context, string, json.RawMessage) (json.RawMessage, error)
}, raw json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 {
		return nil, errors.New("expected a Sessionbus action and arguments object")
	}
	var action string
	var args map[string]json.RawMessage
	if json.Unmarshal(fields["action"], &action) != nil || !slices.Contains(kit.Actions, action) || json.Unmarshal(fields["arguments"], &args) != nil || args == nil {
		return nil, errors.New("expected a Sessionbus action and arguments object")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return owner.Action(ctx, action, fields["arguments"])
}
