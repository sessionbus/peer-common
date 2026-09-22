# Shared-support extraction

Frozen input: peers commit `710e5d33369cba4fb9468cd24fea0fe844a0219d`.
The repository was copied before pruning; the original product history and
release assets remain available in `sessionbus/codex-peer`.

The complete retained support set is listed in `PRESERVED-FILES.json`:
32 source/test/fixture files, 69 original test functions. Of those files,
27 are byte-identical and 5 have only Go import relocation and gofmt changes.
File execution modes and every original test body are preserved. Package paths
move from `wrappers/{host,mcp}` and `internal/{peerversion,testsocket}` to the
corresponding top-level library directories. The explicit legacy maintenance
utility keeps its original path and behavior.

The repository boundary test and workflows are scoped to this library. Local
verification passed the complete retained tests, race detector, vet, lint,
workflow syntax checks, and `go mod tidy -diff`. Consumer and installed checks
are separate obligations; these results alone do not claim installed acceptance.

Copied monorepo tags describe the old module, not this library. They must not
be published as peer-common releases. Their original objects remain in the
preserved source mirror and original Codex repository; peer-common starts its
own module version history. Consumers use a specific reviewed pseudo-version,
with its checksum, and never a moving branch or filesystem replacement.

Codex is the first consumer. Its native plugin identity, native arguments,
configuration, grants and runtime contracts must remain unchanged while its
shared imports and linker variable paths move to the pinned module. Other
products are extracted and verified independently afterwards.
