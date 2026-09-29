// SPDX-License-Identifier: MIT
package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestSendRejectsUnknownSummaryBeforeDispatch(t *testing.T) {
	calls := 0
	const message = "Preserve the complete instruction text."
	caller := kit.NewCaller(func(_ context.Context, method string, params any) (json.RawMessage, error) {
		calls++
		request, ok := params.(*kit.MessageSendRequest)
		if method != "message.send" || !ok || request.Target != "peer@builder" || request.Message != message {
			t.Fatalf("unexpected dispatch: %s %#v", method, params)
		}
		return json.RawMessage(`{"message_id":"m1","deliveries":[]}`), nil
	})
	_, err := CallTool(context.Background(), caller, json.RawMessage(`{"action":"send","arguments":{"target":"peer@builder","message":"Preserve the complete instruction text.","summary":"Brief label"}}`))
	if err == nil || !strings.Contains(err.Error(), `"summary" is not allowed`) || calls != 0 {
		t.Fatalf("invalid send: error=%v calls=%d", err, calls)
	}
	result, err := CallTool(context.Background(), caller, json.RawMessage(`{"action":"send","arguments":{"target":"peer@builder","message":"Preserve the complete instruction text."}}`))
	if err != nil || calls != 1 || !strings.Contains(string(result), `"message_id":"m1"`) {
		t.Fatalf("corrected explicit send: result=%s error=%v calls=%d", result, err, calls)
	}
}

func TestAdvertisedArgumentsMatchPublicActionFieldUnion(t *testing.T) {
	var schema struct {
		Definitions map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(protocol.SessionSchema, &schema); err != nil {
		t.Fatal(err)
	}
	expected := map[string]json.RawMessage{}
	for _, name := range []string{"SessionListRequest", "MessageSendRequest", "LaneDescribeRequest", "LaneSpawnRequest", "TraceConfigureRequest", "TurnRunRequest", "WaitRequest", "RunRef", "TurnInterruptRequest", "SessionCloseRequest"} {
		definition, ok := schema.Definitions[name]
		if !ok {
			t.Fatalf("missing kit definition %s", name)
		}
		for key, value := range definition.Properties {
			expected[key] = value
		}
	}
	advertised := argumentSchema()
	properties := advertised["properties"].(map[string]any)
	if advertised["additionalProperties"] != false || len(properties) != len(expected) {
		t.Fatal("argument field set is not closed or differs from the kit")
	}
	for key := range properties {
		if _, ok := expected[key]; !ok {
			t.Fatalf("unrecognized advertised field %s", key)
		}
		assertArgumentType(t, key, properties[key].(map[string]any), expected[key])
	}
	if _, ok := properties["summary"]; ok {
		t.Fatal("summary must not be advertised")
	}
	open := properties["open"].(map[string]any)
	openProperties := open["properties"].(map[string]any)
	if open["additionalProperties"] != false || len(openProperties) != len(schema.Definitions["SessionOpenOptions"].Properties) {
		t.Fatal("Open fields differ from kit")
	}
	for key := range openProperties {
		if _, ok := schema.Definitions["SessionOpenOptions"].Properties[key]; !ok {
			t.Fatalf("unknown Open field %s", key)
		}
		assertArgumentType(t, key, openProperties[key].(map[string]any), schema.Definitions["SessionOpenOptions"].Properties[key])
	}
	tool := Tool().(map[string]any)
	input := tool["inputSchema"].(map[string]any)
	args := input["properties"].(map[string]any)["arguments"].(map[string]any)
	if args["additionalProperties"] != false {
		t.Fatal("tools/list does not publish closed arguments")
	}
}

func assertArgumentType(t *testing.T, key string, advertised map[string]any, raw json.RawMessage) {
	t.Helper()
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	expected := wire["type"]
	if wire["$ref"] == "#/$defs/SessionOpenOptions" {
		expected = "object"
	}
	if values, ok := wire["enum"].([]any); ok {
		expected = "string"
		actual, _ := advertised["enum"].([]string)
		if len(actual) != len(values) {
			t.Fatalf("%s enum differs from kit", key)
		}
		for i, value := range values {
			if actual[i] != value {
				t.Fatalf("%s enum differs from kit", key)
			}
		}
	}
	if advertised["type"] != expected {
		t.Fatalf("%s type = %v, want %v", key, advertised["type"], expected)
	}
	if expected == "array" && advertised["items"].(map[string]any)["type"] != wire["items"].(map[string]any)["type"] {
		t.Fatalf("%s item type differs from kit", key)
	}
}
