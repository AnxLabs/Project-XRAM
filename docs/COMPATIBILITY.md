# Compatibility and portability

XRAM is an open-source experiment intended for varied machines and contributors. The first two benchmark computers are test environments, not required hardware. The current implementation is a single-server Python prototype; all environments below are **experimental**, not production certifications.

## What has been verified

| Environment | Evidence | Boundary |
| --- | --- | --- |
| Windows and Ubuntu, Python 3.10–3.13 | The repository's GitHub Actions matrix runs the automated unit suite on both operating systems. | Unit tests alone do not prove a particular LAN, firewall, CPU architecture, or workload. See the latest CI run for current status. |
| Windows 11 client and Debian 13 server, Python 3.11/3.13 | Isolated IPv4 LAN cache and fixed-search experiments with TLS, authentication, failure cases and synthetic fixtures. | This particular Ethernet/Wi-Fi home LAN is one test setup; results do not generalize to other devices or interconnects. |
| Windows 11 and Debian 13 loopback, Python 3.11/3.13 | An isolated real TCP/TLS loopback test stored, retrieved, searched and deleted a synthetic vector on each OS. | It does not establish cross-machine Windows-server/Debian-client operation, or coverage of other operating systems. The test skips if OpenSSL is unavailable. |
| Local Python APIs | Standard-library implementation and automated tests cover the store, scheduler, codec, cache selector and fixed search client. | Experimental API and protocol; no backward-compatibility guarantee or independent security audit. |

The server currently creates an **IPv4** listener and matches incoming IPv4 peer addresses against its allowlist. The client accepts a configurable host and verifies the certificate name plus certificate pin; hostnames require an appropriate certificate and an IPv4-reachable server. IPv6 listening, NAT traversal, internet deployment and automatic discovery have not been implemented or verified.

## Planned or unverified

macOS, other Linux distributions, other Windows versions, ARM, other CPU architectures, and other Python versions may work but are not verified by the recorded tests. A benchmark can still run where timing and sockets work even if process-memory statistics are unavailable; it reports missing measurements instead of treating them as zero. Platform-specific system RAM, swap management, GPU/accelerator memory, kernel paging, RDMA and device drivers are outside the current prototype.

XRAM does not currently provide transparent memory expansion, paging for unmodified applications, shared virtual memory, durable remote memory, multiple memory nodes, discovery, replication or failover. Those are possible future research directions, not supported features or a delivery commitment. Contributors should discuss architecture, security and measurable use cases before adding them.

## Configuration

The safe default is the standard IPv4 loopback address; it is not tied to the benchmark machines. For LAN use, select your own `--bind`, repeat `--peer` for each permitted **numeric IPv4** client address, set `--port`, and provide a certificate with a matching IP subject alternative name. The server compares the actual source IPv4 address literally; peer hostnames and CIDR ranges are not supported. Client `--host`, port, certificate and token are supplied by the user. Resource controls are configurable with explicit ceilings; see [installation](INSTALLATION.md) and `python xram.py server --help`.

The repository does not require AnxLabs services, private source files, cloud services, a specific network interface, or the original benchmark hardware. Live network results depend on each environment. Use only an isolated trusted network and never expose the prototype directly to the internet.
