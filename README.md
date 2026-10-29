# Project XRAM by AnxLabs

XRAM is an **experimental remote-memory cache and fixed-function compute prototype** for trusted local networks. It stores bounded, temporary byte objects in another computer's process memory and can optionally rank vectors where they are stored. It is **not transparent physical RAM expansion**, operating-system swap, shared virtual memory, durable storage, or a production service.

## Current status

Research is paused, not abandoned. The prototype demonstrates authenticated, encrypted transfers, content verification, bounded storage, expiry, a limited vector search, and an optional bounded search queue. Our measurements show that local RAM and a warm local SSD can be much faster than remote access. A queue can improve burst completion without increasing search throughput. See [benchmark results](benchmarks/README.md) for successful and unsuccessful cases and their limitations.

The public repository contains the reusable XRAM core and synthetic benchmarks. The original AnxLabs research history, internal application integrations, private source documents and derived datasets are not distributed here.

## Get started

Python 3.10+ is required; the runtime and automated tests use the Python standard library. OpenSSL is needed only to create local test credentials. The server defaults to loopback and accepts loopback clients. Follow the [installation guide](docs/INSTALLATION.md) to create private credentials, run a local smoke test, and configure an isolated LAN test. The server is not a system service and clears its data on exit.

```sh
python -m unittest -v
```

See [architecture](docs/ARCHITECTURE.md), [benchmark methodology](benchmarks/README.md), [security policy](SECURITY.md), and [contributing](CONTRIBUTING.md). Apache License 2.0 applies; see [LICENSE](LICENSE).

## Boundaries

- Stored data is ephemeral; keep an authoritative copy elsewhere.
- TLS authenticates the server and encrypts traffic; one shared token authenticates all allowed clients. There are no per-user roles, replication, durable storage, or multi-node routing.
- Payload limits do not cap total process RSS, pin physical memory, or prevent operating-system swapping.
- Search is disabled by default and only accepts a predefined bounded vector operation. Cancellation is cooperative; a disconnected client is not automatically canceled during a queued or active search.
- Do not expose the service to the internet or use it for sensitive or production data. It has not undergone an independent security audit.
