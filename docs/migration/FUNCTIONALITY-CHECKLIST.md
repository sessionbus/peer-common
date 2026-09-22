# Stable shared-support checklist

Baseline: `710e5d33369cba4fb9468cd24fea0fe844a0219d`.
These requirements keep their IDs. Failures remain failures; do not delete tests
or change requirements to manufacture a pass. Source/test bodies are preserved
except namespace relocation. Product callers must pin the reviewed revision.

| ID | Retained behavior | Existing verification |
|---|---|---|
| C01 | Token-presence lane mode, groups/name/native argv and owned launch environment | host/launch_test.go |
| C02 | Owned child close/output drain and failure handling | host/launch_test.go |
| C03 | Private endpoint authority, stale-path replacement and cancellation | host/endpoint_test.go |
| C04 | Session lock contention/rename/inode and crash-stale handling | host/lock_test.go |
| C05 | Exact native message envelope bytes/escaping | host/render_test.go and original JSON fixture |
| C06 | Handoff FIFO, admission/claim/terminal races, failed-creation restoration | host/handoff_test.go |
| C07 | One public tool, exact action arguments, per-request identity and self_info, trace response | mcp/*_test.go |
| C08 | MCP frame/work/response budgets, cancellation, EOF and no late replies | mcp/sessionbus_bounds_test.go; server_test.go |
| C09 | Native hidden reports, inactive zero-tool helper and lane caller/result errors | mcp/sessionbus_test.go; lane_test.go |
| C10 | Native-free public version flags; exact private-alias argument boundary | peerversion/version_test.go plus consumer entry tests |
| C11 | Short real runtime socket paths, symlink resolution and owned test cleanup | testsocket/directory_test.go |
| C12 | Explicit legacy cleanup with preservation of unrelated/new integrations | scripts/cleanup-legacy/*_test.go |
| C13 | Independent Go module, public SDK only, no other-product runtime | architecture_test.go; go mod tidy -diff; build/vet/lint |
| C14 | No lost shared regression/source files | PRESERVED-FILES.json and exact normalized comparison |
| C15 | Consumer compatibility before a pin advances | Each affected peer's retained suite and installed evidence where the changed boundary requires it |

No installed native behavior is claimed by this library alone. The first
consumer is Codex; later products retain their own distinct native contracts.
No release tag is reused, no moving branch dependency, and no automatic consumer
upgrade is part of this extraction.
