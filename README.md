# Sessionbus peer common support

Shared Go support for [Sessionbus peers](https://github.com/sessionbus).
This is a mechanical extraction from peers commit
`710e5d33369cba4fb9468cd24fea0fe844a0219d`, retaining the implementation and its
regression tests. It does not contain native product adapters or plugins.

| Package | Existing responsibility |
|---|---|
| `host` | Managed launch arguments/environment, owned child lifecycle, private endpoints, session locks, message rendering and queue handoff |
| `mcp` | Public Sessionbus tool declaration/dispatch, caller metadata, native MCP protocol, cancellation and resource bounds |
| `peerversion` | Public wrapper version flags and source identity; private aliases remain untouched |
| `testsocket` | Short, production-shaped socket directories used only by tests |

The Bus daemon/protocol/SDK remain in [sessionbus](https://github.com/sessionbus/sessionbus).
The released SDK dependency keeps its Go module path
`github.com/antst/sessionbus/bus/sdk/go`; repository transfer does not rename that
released module. No daemon internals, native model, Node.js or npm is required here.

Product-specific transports, permissions, native state, plugins and installers
belong to their peer repositories. OpenCode/Kilo and Pi/OMP family code stays
with those paired products. No generic native-product abstraction is introduced.

Consumers must pin an exact module version or pseudo-version. A common change
must pass this suite and the retained suites of affected consumers before any
consumer advances its pin; publishing common code must not silently upgrade peers.
The product build must stamp its OWN release/revision via:

```text
-X github.com/sessionbus/peer-common/peerversion.Release=<product release>
-X github.com/sessionbus/peer-common/peerversion.Revision=<product commit>
```

Common-module version and product release identity are separate.

## Build and verify

Go 1.24 or later:

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

See the [stable functionality checklist](docs/migration/FUNCTIONALITY-CHECKLIST.md)
and [frozen source/test inventory](docs/migration/PRESERVED-FILES.json).
Native installed validation belongs to the consuming products, using their actual
permanent installation. Common unit tests are not a native wake acceptance claim.

The [legacy cleanup utility](docs/development/legacy-cleanup.md) remains under
`scripts/cleanup-legacy` as an explicitly invoked maintenance tool with its tests;
it is not linked into peer executables or run automatically.

## Source history

This repository was copied in full before pruning. Product history remains in
Git and the canonical repositories: [Codex](https://github.com/sessionbus/codex-peer),
[Claude](https://github.com/sessionbus/claude-peer),
[Grok](https://github.com/sessionbus/grok-peer), [Qwen](https://github.com/sessionbus/qwen-peer),
[OpenCode/Kilo](https://github.com/sessionbus/opencode-kilo),
[Pi/OMP](https://github.com/sessionbus/pi-omp).
Inherited monorepo release tags are not common-library releases; consumers must
not use them as module versions. The original tags/assets remain in codex-peer.
