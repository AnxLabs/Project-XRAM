# Architecture

XRAM is a single-process, volatile object store plus an optional fixed computation. A client connects through TLS, validates and pins the configured certificate, and sends a token on every request. The server checks an explicit peer allowlist and handles bounded framed JSON control messages and byte payloads. Objects have length, SHA-256, expiration and an LRU position. Synchronized accounting includes reserved uploads and objects still held by readers. Stored-payload capacity and object count are bounded; total process memory and operating-system paging are not.

The optional 256-dimensional float32 dot-product search is enabled explicitly. Its searchable object size is bounded, returned top-k is at most ten, and non-finite values are rejected. It executes a fixed operation; clients cannot supply code. Default concurrency is one active search and no queue. An optional FIFO queue has bounded length and wait time; authenticated requests can cancel a known outstanding ID. Active cancellation is cooperative. A disconnected client does not automatically cancel work and cancellation can race with completion.

A shared token has equal authority over all objects; there is no per-client isolation, revocation protocol, independent security audit, persistent state, replication, cluster routing or multi-node support. Server-provided checksums detect transmission errors, but trusted expected hashes must come from a separate trusted source. The source data is lost on server exit, so keep an authoritative copy and handle misses or failures explicitly.

## Extension points and research direction

`xram.py` owns the bounded store, authenticated protocol and server/client entry points. `search_scheduler.py` controls bounded admission and cancellation; `compute_near_memory.py` provides a client for the fixed search; `xram_codec.py` and `adaptive_cache.py` are optional application-side helpers. These boundaries help contributors experiment independently but are not stable plugin interfaces. Extend them only with explicit protocol, security, resource and compatibility tests; never accept arbitrary remote code.

The long-term goal is to investigate usable distributed memory across computers. The present object cache and bounded compute operation are partial experiments toward that goal; transparent physical-memory expansion and multi-node orchestration do not exist in this version. Reopening or extending the research should follow representative workloads, reproducible cross-platform evidence and explicit limits, not the hardware in the initial benchmarks.
