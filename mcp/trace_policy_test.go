// SPDX-License-Identifier: MIT

package mcp

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSpawnResultTraceSurvivesSDKAndMCP(t *testing.T) {
	for _, mode := range []string{"", "off", "events", "content"} {
		for _, action := range []string{"fresh", "resume"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				policy := map[string]any{"persistent": false, "auto_close_ms": float64(60000), "idle_message": "run", "notify": true}
				if mode != "" {
					policy["trace"] = mode
				}
				want := map[string]any{"session_id": "child@local", "policy": policy}
				raw, err := json.Marshal(want)
				check(t, err == nil, "marshal: %v", err)
				backend := &fakeBackend{result: raw}
				args := `{"name":"child","product":"fixture-worker","open":{}}`
				if action == "resume" {
					args = `{"resume_session_id":"child@local"}`
				}
				response, err := serveOne(&Server{Backend: backend}, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sessionbus","arguments":{"action":"spawn","arguments":`+args+`}}}`)
				check(t, err == nil, "serve: %v", err)
				check(t, response["error"] == nil, "response = %#v", response)
				result := response["result"].(map[string]any)
				check(t, backend.method == "lane.spawn" && reflect.DeepEqual(result["structuredContent"], want), "method/result = %s / %#v", backend.method, result)
				var textResult map[string]any
				err = json.Unmarshal([]byte(result["content"].([]any)[0].(map[string]any)["text"].(string)), &textResult)
				check(t, err == nil && reflect.DeepEqual(textResult, want), "text result = %#v, %v", textResult, err)
			})
		}
	}
}
