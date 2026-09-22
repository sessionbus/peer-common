# Common Sessionbus MCP lifetime and limits

`ServeSessionbus` and `ServeInactiveSessionbus` own their input and any output
implementing `io.Closer`. EOF, malformed transport framing, output failure, or
capacity exhaustion cancels calls, closes those streams, ends the owner, and
joins admitted response work. EOF does not promise delivery of queued replies.
Callers must read required replies before closing input. Real sockets and pipes
support write interruption through Close; arbitrary nonclosable blocking writers
are unsupported. Action callbacks must settle after cancellation/owner teardown.

All replies, including initialize, errors and inactive replies, use bounded
asynchronous work. Reading never waits for a response slot or the serialized
writer. Hidden report admission remains synchronous and reader-ordered;
completion waits are cancellation-aware. `Initialized()` runs only after a
successful initialize write while the connection remains live. That callback
must return promptly. Successful action completion still wins request
cancellation while the connection lives; cancellation does not consume any
retained run result.

Limits are 256 admitted response operations, 2 MiB per input frame (including
newline when present), 8 MiB per encoded output frame, and 32 MiB aggregate
retained payload. Each operation charges its ingress bytes through completion;
returned result/error data is conservatively charged before serialization or
waiting for the writer. Exceeding a limit closes the connection rather than
queuing an overload reply. Completed, failed and cancelled work releases its
reservation. No ceiling-sized buffers are preallocated.

Encoding and writing are serialized. A valid bus result may contain raw `<`
bytes, each expanding sixfold in an outer MCP JSON string, so the 8 MiB output
ceiling accommodates the 1 MiB bus-frame limit. The retained-payload budget is
not an exact RSS limit: Go objects, parsed field copies, the current input frame,
and one serializer's temporary allocations are additional. Encoded size is
checked before Write; an oversized encoded response is discarded.
