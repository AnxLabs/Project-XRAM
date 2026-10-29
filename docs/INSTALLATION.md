# Installation and isolated use

## Requirements

Python 3.10 or newer on both machines and OpenSSL on the server. No third-party Python runtime packages are required. Use a trusted private network and a disposable test directory. The two files `xram.py` and `search_scheduler.py` must be together on the server. The server defaults to the standard IPv4 loopback address `127.0.0.1`, and its default permitted peer is also loopback; these are safe defaults, not hardware-specific addresses. No firewall rules, system services, swap, or kernel changes are needed.

## Create test credentials

On the server, with a private directory and a known IP address, generate a token and a self-signed certificate whose IP subject alternative name matches the address the client will use. This example is for a local-only loopback test:

```sh
umask 077
mkdir -m 700 xram-test-credentials
openssl rand -hex 32 > xram-test-credentials/token
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1 \
  -keyout xram-test-credentials/key.pem -out xram-test-credentials/cert.pem \
  -subj /CN=127.0.0.1 -addext subjectAltName=IP:127.0.0.1
```

Keep the token and private key private. For a LAN test, substitute the server's private LAN address in both certificate fields, securely transfer the public certificate and token to the intended client, and keep the private key on the server. Generate fresh credentials for each isolated environment; never place them in the repository or command-line values.

## Start and stop

In the directory with the server files:

```sh
python3 xram.py server --token xram-test-credentials/token \
  --cert xram-test-credentials/cert.pem --key xram-test-credentials/key.pem
```

For a LAN test explicitly add `--bind SERVER_PRIVATE_IP --peer CLIENT_PRIVATE_IP --port PORT`; the server accepts repeated `--peer` flags containing numeric IPv4 addresses. See [compatibility](COMPATIBILITY.md) for verified environments and current IPv4 limitations. Use only a free port and an isolated endpoint. `--limit` controls stored payload bytes (default 256 MiB), `--ttl` controls expiration in seconds, and `--workers` bounds connections. Optional `--enable-search` enables predefined vector ranking; `--search-limit`, `--search-workers`, `--search-queue`, and `--search-wait-ms` impose additional bounds. The queue is off by default. These are not process-wide CPU or RSS limits.

From the client, verify connectivity with its local copies of the token and certificate:

```sh
python xram.py client ping --host SERVER_PRIVATE_IP --port PORT --token /private/token --cert /private/cert.pem
```

Stop the foreground server with Ctrl+C. If detached intentionally, identify and stop only that specific process. Do not assume closing an SSH terminal stops it. Verify exact paths and authorized cleanup procedures before disposing of temporary credentials. Test data disappears on server exit.

For a search example, see the client `compute_near_memory.Session` and the synthetic benchmark. Search requires a trusted expected artifact digest; results are not a cryptographic proof of honest computation.

## Verify your environment

Run `python -m unittest -v`. The optional end-to-end loopback TLS test uses an `openssl` executable on `PATH` to generate a disposable certificate; it reports a skip when OpenSSL is absent. Unit tests and a local loopback success do not prove a cross-machine network route. See [compatibility](COMPATIBILITY.md) before treating another platform or topology as tested.
