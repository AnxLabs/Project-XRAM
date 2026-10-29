"""XRAM: bounded, ephemeral remote byte cache over pinned TLS."""
import argparse
from collections import OrderedDict
import hashlib
import heapq
import math
import hmac
import json
import os
import socket
import ssl
import struct
import threading
import time
from search_scheduler import Scheduler

MAX_BYTES = 256 * 1024 * 1024
MAX_HEADER = 4096
MAX_OBJECTS = 128
TIMEOUT = 15
MAX_CONNECTION_REQUESTS = 32
MAX_CONNECTION_SECONDS = 60
MAX_SEARCH_BYTES = 4 * 1024 * 1024
HARD_MAX_SEARCH_BYTES = 32 * 1024 * 1024
SEARCH_DIM = 256

def read_exact(sock, count, deadline=None):
    out = bytearray(count)
    view = memoryview(out)
    while view:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise TimeoutError('connection deadline exceeded')
            sock.settimeout(min(TIMEOUT, remaining))
        n = sock.recv_into(view)
        if not n:
            raise EOFError('connection closed' if len(view) == count else 'truncated data')
        view = view[n:]
    return out

def send_msg(sock, obj):
    data = json.dumps(obj, separators=(',', ':')).encode()
    if len(data) > MAX_HEADER:
        raise ValueError('header too large')
    sock.sendall(struct.pack('!I', len(data)) + data)

def recv_msg(sock, deadline=None):
    length = struct.unpack('!I', read_exact(sock, 4, deadline))[0]
    if not 0 < length <= MAX_HEADER:
        raise ValueError('invalid header length')
    obj = json.loads(read_exact(sock, length, deadline))
    if not isinstance(obj, dict):
        raise ValueError('invalid message')
    return obj

class SearchCanceled(Exception):
    pass

def valid_search_id(value):
    return isinstance(value, str) and len(value) == 32 and all(c in '0123456789abcdef' for c in value)

class Store:
    def __init__(self, limit=MAX_BYTES, ttl=300, *, enable_search=False, search_limit=MAX_SEARCH_BYTES, search_workers=1, search_queue=0, search_wait_ms=1000):
        if not 0 < limit <= MAX_BYTES or not 1 <= ttl <= 86400:
            raise ValueError('invalid limit or TTL')
        self.limit, self.ttl, self.items, self.used = limit, ttl, OrderedDict(), 0
        if type(search_limit) is not int or not 1024 <= search_limit <= HARD_MAX_SEARCH_BYTES:
            raise ValueError('invalid search limit')
        self.search_limit = search_limit
        self.enable_search = enable_search
        self.scheduler = Scheduler(search_workers, search_queue, search_wait_ms)
        self.search_slot = self.scheduler.slots
        self.lock = threading.RLock()
        self.started = time.monotonic()
        self.reserved = 0
        self.pending = set()
        self.counters = dict(hits=0, misses=0, puts=0, deletes=0, evictions=0,
                             expirations=0, rejected=0, uploaded_bytes=0, downloaded_bytes=0)

    def _remove(self, key, reason):
        entry = self.items.pop(key, None)
        if entry is None: return False
        entry['retired'] = True
        if entry['readers'] == 0: self.used -= len(entry['data'])
        if reason: self.counters[reason] += 1
        return True

    def expire(self):
        with self.lock:
            now = time.monotonic()
            for key in [k for k, entry in self.items.items() if entry['deadline'] <= now]:
                self._remove(key, 'expirations')

    def delete(self, key):
        with self.lock:
            return self._remove(key, 'deletes')

    def snapshot(self):
        with self.lock:
            self.expire()
            stats = dict(self.counters, used=self.used, reserved=self.reserved,
                         limit=self.limit, objects=len(self.items), pending=len(self.pending),
                         uptime_seconds=round(time.monotonic() - self.started, 3))
        stats.update(self.scheduler.snapshot())
        # Linux reports actual process memory including non-payload overhead and swap.
        try:
            with open('/proc/self/status', encoding='ascii') as f:
                for line in f:
                    if line.startswith(('VmRSS:', 'VmSwap:')):
                        k, v = line.split(':', 1)
                        stats[k + '_bytes'] = int(v.split()[0]) * 1024
        except OSError: pass
        return stats

    def serve(self, conn, token, deadline=None):
        self.expire()
        request = recv_msg(conn, deadline)
        supplied = request.get('token')
        if not isinstance(supplied, str) or not supplied.isascii() or not hmac.compare_digest(supplied, token):
            send_msg(conn, {'ok': False, 'error': 'unauthorized'})
            return
        op, key = request.get('op'), request.get('key')
        if op in ('ping', 'stats'):
            send_msg(conn, dict(ok=True, **self.snapshot()))
        elif op == 'cancel_search':
            request_id = request.get('request_id')
            if not self.enable_search:
                send_msg(conn, {'ok': False, 'error': 'search disabled'})
            elif set(request) - {'op', 'token', 'request_id', 'keep'} or not valid_search_id(request_id):
                send_msg(conn, {'ok': False, 'error': 'invalid search id'})
            else:
                send_msg(conn, {'ok': True, 'canceled': self.scheduler.cancel(request_id)})
        elif not isinstance(key, str) or not 1 <= len(key) <= 128 or not key.isascii():
            send_msg(conn, {'ok': False, 'error': 'invalid key'})
        elif op == 'put':
            size = request.get('size')
            ttl = request.get('ttl', self.ttl)
            evict = request.get('evict', False)
            if type(size) is not int or size < 0 or size > self.limit or type(ttl) is not int or not 1 <= ttl <= 86400 or type(evict) is not bool:
                send_msg(conn, {'ok': False, 'error': 'invalid size'})
            else:
                with self.lock:
                    self.expire()
                    error = None
                    if key in self.items or key in self.pending:
                        error = 'key exists; delete first'
                    else:
                        while evict and (len(self.items) + len(self.pending) >= MAX_OBJECTS or self.used + self.reserved + size > self.limit):
                            victim = next((k for k, e in self.items.items() if not e['readers']), None)
                            if victim is None: break
                            self._remove(victim, 'evictions')
                        if len(self.items) + len(self.pending) >= MAX_OBJECTS or self.used + self.reserved + size > self.limit:
                            error = 'capacity exceeded'
                    if error:
                        self.counters['rejected'] += 1
                    else:
                        self.pending.add(key)
                        self.reserved += size
                if error:
                    send_msg(conn, {'ok': False, 'error': error})
                    return
                reserved = True
                payload = view = None
                try:
                    try: payload = bytearray(size)
                    except MemoryError:
                        send_msg(conn, {'ok': False, 'error': 'allocation failed'})
                        return
                    send_msg(conn, {'ok': True, 'ready': True})
                    view = memoryview(payload)
                    upload_deadline = min(time.monotonic() + 120, deadline) if deadline is not None else time.monotonic() + 120
                    while view:
                        remaining = upload_deadline - time.monotonic()
                        if remaining <= 0: raise TimeoutError('upload deadline exceeded')
                        conn.settimeout(min(TIMEOUT, remaining))
                        n = conn.recv_into(view)
                        if not n: raise EOFError('incomplete upload')
                        view = view[n:]
                    digest = hashlib.sha256(payload).hexdigest()
                    with self.lock:
                        self.items[key] = dict(data=payload, deadline=time.monotonic()+ttl, readers=0, retired=False, digest=digest)
                        self.used += size
                        self.counters['puts'] += 1
                        self.counters['uploaded_bytes'] += size
                        payload = view = None
                        self.pending.remove(key)
                        self.reserved -= size
                        reserved = False
                    send_msg(conn, {'ok': True, 'sha256': digest})
                finally:
                    if reserved:
                        payload = view = None
                        with self.lock:
                            self.pending.remove(key)
                            self.reserved -= size
        elif op == 'get':
            with self.lock:
                self.expire()
                item = self.items.get(key)
                if item is None: self.counters['misses'] += 1
                else:
                    self.items.move_to_end(key)
                    item['readers'] += 1
                    self.counters['hits'] += 1
            if item is None:
                send_msg(conn, {'ok': False, 'error': 'not found'})
            else:
                try:
                    data = item['data']
                    send_msg(conn, {'ok': True, 'size': len(data), 'sha256': item['digest']})
                    conn.sendall(data)
                    with self.lock: self.counters['downloaded_bytes'] += len(data)
                finally:
                    data = None
                    with self.lock:
                        item['readers'] -= 1
                        if item['retired'] and not item['readers']:
                            size = len(item['data'])
                            item['data'] = None
                            self.used -= size
        elif op == 'search':
            # Explicitly enabled, bounded and fixed operation; no executable input.
            index, top = request.get('query_index'), request.get('top')
            request_id = request.get('request_id')
            if not self.enable_search:
                send_msg(conn, {'ok': False, 'error': 'search disabled'})
            elif set(request) - {'op', 'token', 'key', 'query_index', 'top', 'keep', 'request_id'} or (request_id is not None and not valid_search_id(request_id)) or type(index) is not int or type(top) is not int or not 1 <= top <= 10:
                send_msg(conn, {'ok': False, 'error': 'invalid search'})
            else:
                ticket, error, wait_ms = self.scheduler.acquire(request_id, deadline)
                if error:
                    send_msg(conn, {'ok': False, 'error': error})
                    return request.get('keep') is True
                item = None
                try:
                    with self.lock:
                        self.expire()
                        item = self.items.get(key)
                        if item is not None:
                            item['readers'] += 1
                            self.items.move_to_end(key)
                    if item is None:
                        send_msg(conn, {'ok': False, 'error': 'not found'})
                    elif not 0 < len(item['data']) <= self.search_limit or len(item['data']) % (SEARCH_DIM * 4) or not 0 <= index < len(item['data']) // (SEARCH_DIM * 4):
                        send_msg(conn, {'ok': False, 'error': 'invalid vector data or index'})
                    else:
                        data = item['data']
                        query = struct.unpack_from('<256f', data, index * 1024)
                        def ranked():
                            if not all(math.isfinite(v) for v in query): raise ValueError('invalid vector')
                            for row_index, row in enumerate(struct.iter_unpack('<256f', data)):
                                if row_index % 64 == 0 and ticket.canceled.is_set(): raise SearchCanceled()
                                if not all(math.isfinite(v) for v in row): raise ValueError('invalid vector')
                                yield (sum(a*b for a,b in zip(row, query)), row_index)
                        try:
                            results = heapq.nlargest(top, ranked())
                        except SearchCanceled:
                            send_msg(conn, {'ok': False, 'error': 'search canceled'})
                        except ValueError:
                            send_msg(conn, {'ok': False, 'error': 'invalid vector data or index'})
                        else:
                            if ticket.canceled.is_set(): send_msg(conn, {'ok': False, 'error': 'search canceled'})
                            else: send_msg(conn, {'ok': True, 'sha256': item['digest'], 'results': results, 'queue_wait_ms': round(wait_ms, 3)})
                finally:
                    if item is not None:
                        with self.lock:
                            item['readers'] -= 1
                            if item['retired'] and not item['readers']:
                                self.used -= len(item['data'])
                                item['data'] = None
                    self.scheduler.release(ticket)
        elif op == 'delete':
            send_msg(conn, {'ok': self.delete(key)})
        else:
            send_msg(conn, {'ok': False, 'error': 'unknown operation'})
        # Legacy clients omit this flag and retain one-request-per-connection behavior.
        return request.get('keep') is True

def token_from(path):
    with open(path, encoding='ascii') as f:
        value = f.read().strip()
    if len(value) < 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('token must be at least 64 lowercase hex characters')
    return value

def peer_allowlist(peers):
    if peers is None: return {'127.0.0.1'}
    return {peers} if isinstance(peers, str) else set(peers)

def server(args):
    token = token_from(args.token)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(args.cert, args.key)
    store = Store(args.limit, args.ttl, enable_search=args.enable_search, search_limit=args.search_limit, search_workers=args.search_workers, search_queue=args.search_queue, search_wait_ms=args.search_wait_ms)
    peers = peer_allowlist(args.peer)
    slots = threading.BoundedSemaphore(args.workers)
    def handle(raw):
        try:
            with raw:
                raw.settimeout(TIMEOUT)
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with context.wrap_socket(raw, server_side=True) as conn:
                    deadline = None
                    for _ in range(MAX_CONNECTION_REQUESTS):
                        remaining = deadline - time.monotonic() if deadline is not None else TIMEOUT
                        if remaining <= 0: break
                        conn.settimeout(min(TIMEOUT, remaining))
                        try:
                            if not store.serve(conn, token, deadline): break
                            if deadline is None: deadline = time.monotonic() + MAX_CONNECTION_SECONDS
                        except EOFError as exc:
                            if deadline is not None and str(exc) == 'connection closed': break
                            raise
        except (OSError, ValueError, EOFError, ssl.SSLError, json.JSONDecodeError) as exc:
            print('request failed: ' + type(exc).__name__, flush=True)
        finally:
            slots.release()
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((args.bind, args.port))
        listener.listen(4)
        listener.settimeout(1)
        print('XRAM listening on %s:%d' % (args.bind, args.port), flush=True)
        while True:
            store.expire()
            try:
                raw, peer = listener.accept()
            except socket.timeout:
                continue
            if peer[0] not in peers or not slots.acquire(blocking=False):
                raw.close()
                continue
            threading.Thread(target=handle, args=(raw,), daemon=True).start()

class Client:
    def __init__(self, host, port, cert, token):
        self.host, self.port, self.token = host, port, token
        self.context = ssl.create_default_context(cafile=cert)
        self.context.minimum_version = ssl.TLSVersion.TLSv1_2
        # Pin the exact certificate as well as validating its name and chain.
        with open(cert, encoding='ascii') as f:
            self.pin = hashlib.sha256(ssl.PEM_cert_to_DER_cert(f.read())).digest()

    def connect(self):
        raw = socket.create_connection((self.host, self.port), timeout=TIMEOUT)
        try:
            conn = self.context.wrap_socket(raw, server_hostname=self.host)
            if not hmac.compare_digest(hashlib.sha256(conn.getpeercert(binary_form=True)).digest(), self.pin):
                conn.close()
                raise ssl.SSLError('certificate pin mismatch')
            return conn
        except Exception:
            raw.close()
            raise

    def request(self, op, key=None, data=None, *, ttl=None, evict=False):
        with self.connect() as conn:
            msg = {'op': op, 'token': self.token}
            if key is not None: msg['key'] = key
            if data is not None: msg['size'] = len(data)
            if ttl is not None: msg['ttl'] = ttl
            if evict: msg['evict'] = True
            send_msg(conn, msg)
            response = recv_msg(conn)
            if not response.get('ok'):
                raise CacheError(response.get('error', 'operation failed'))
            if op == 'put':
                conn.sendall(data)
                response = recv_msg(conn)
                if not response.get('ok') or response.get('sha256') != hashlib.sha256(data).hexdigest():
                    raise ValueError('upload integrity failure')
            elif op == 'get':
                size = response.get('size')
                if type(size) is not int or not 0 <= size <= MAX_BYTES:
                    raise ValueError('invalid response size')
                data = read_exact(conn, size)
                if hashlib.sha256(data).hexdigest() != response.get('sha256'):
                    raise ValueError('download integrity failure')
                return data
            return response

class CacheError(RuntimeError):
    pass

class CacheClient(Client):
    """Application cache-aside API. Missing keys return None; failures raise."""
    def get(self, key):
        try: return self.request('get', key)
        except CacheError as e:
            if str(e) == 'not found': return None
            raise

    def put(self, key, data, ttl=None):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError('data must be bytes-like')
        return self.request('put', key, data, ttl=ttl, evict=True)

    def delete(self, key): return self.request('delete', key)
    def stats(self): return self.request('stats')

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='mode', required=True)
    s = sub.add_parser('server')
    s.add_argument('--bind', default='127.0.0.1')
    s.add_argument('--peer', action='append', help='allowed IPv4 address; repeat to allow multiple peers (default: 127.0.0.1)')
    s.add_argument('--port', type=int, default=39471); s.add_argument('--token', required=True)
    s.add_argument('--cert', required=True); s.add_argument('--key', required=True)
    s.add_argument('--limit', type=int, default=MAX_BYTES); s.add_argument('--ttl', type=int, default=300)
    s.add_argument('--workers', type=int, choices=range(1, 33), default=8)
    s.add_argument('--enable-search', action='store_true', help='enable bounded predefined vector search')
    s.add_argument('--search-limit', type=int, default=MAX_SEARCH_BYTES, help='maximum searchable object bytes, capped at 32 MiB')
    s.add_argument('--search-workers', type=int, default=1, choices=(1,2))
    s.add_argument('--search-queue', type=int, default=0, choices=range(0,9))
    s.add_argument('--search-wait-ms', type=int, default=1000, choices=range(1,5001), metavar='1..5000')
    c = sub.add_parser('client')
    c.add_argument('operation', choices=['ping', 'stats', 'put', 'get', 'delete'])
    c.add_argument('--host', default='127.0.0.1'); c.add_argument('--port', type=int, default=39471)
    c.add_argument('--token', required=True); c.add_argument('--cert', required=True)
    c.add_argument('--key'); c.add_argument('--file')
    c.add_argument('--ttl', type=int, help='object lifetime in seconds for put')
    c.add_argument('--evict', action='store_true', help='evict least-recently-used objects to make space')
    a = p.parse_args()
    if a.mode == 'server': server(a); return
    client = Client(a.host, a.port, a.cert, token_from(a.token))
    if a.operation in ('put', 'get') and (not a.key or not a.file): p.error('put/get require --key and --file')
    if a.operation == 'delete' and not a.key: p.error('delete requires --key')
    if a.operation == 'put':
        with open(a.file, 'rb') as f: data = f.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES: raise ValueError('file exceeds 256 MiB')
        print(client.request('put', a.key, data, ttl=a.ttl, evict=a.evict))
    elif a.operation == 'get':
        data = client.request('get', a.key)
        with open(a.file, 'xb') as f: f.write(data)
        print('retrieved %d bytes, SHA-256 %s' % (len(data), hashlib.sha256(data).hexdigest()))
    else: print(client.request(a.operation, a.key))

if __name__ == '__main__': main()
