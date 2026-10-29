import socket
import json
import struct
import threading
import time
import unittest
import xram

TOKEN = 'a' * 64

class Tests(unittest.TestCase):
    def setUp(self): self.store = xram.Store(8, 1)

    def test_search_limit_config_preserves_default_and_enforces_hard_cap(self):
        self.assertEqual(xram.Store().search_limit, 4 * 1024 * 1024)
        self.assertEqual(xram.Store(search_limit=8 * 1024 * 1024).search_limit, 8 * 1024 * 1024)
        for bad in (0, True, xram.HARD_MAX_SEARCH_BYTES + 1):
            with self.assertRaises(ValueError): xram.Store(search_limit=bad)
        self.store = xram.Store(4096, 30, enable_search=True, search_limit=1024)
        vector = struct.pack('<256f', *([1.0] + [0.0]*255)) * 2
        self.session(self.req('put', key='v', size=len(vector)), vector)
        self.assertEqual(self.session(self.req('search', key='v', query_index=0, top=1))[0]['error'], 'invalid vector data or index')
        self.store.search_limit=2048
        self.assertTrue(self.session(self.req('search', key='v', query_index=0, top=1))[0]['ok'])

    def test_search_opt_in_auth_validation_and_correctness(self):
        raw = struct.pack('<256f', *([1.0] + [0.0]*255)) + struct.pack('<256f', *([0.5] + [0.0]*255))
        self.store = xram.Store(4096, 30)
        self.session(self.req('put', key='v', size=len(raw)), raw)
        msg = self.req('search', key='v', query_index=0, top=2)
        self.assertEqual(self.session(msg)[0]['error'], 'search disabled')
        self.store.enable_search = True
        self.assertEqual(self.session(dict(msg, token='b'*64))[0]['error'], 'unauthorized')
        self.assertEqual(self.session(dict(msg, query_index=True))[0]['error'], 'invalid search')
        self.assertEqual(self.session(dict(msg, top=11))[0]['error'], 'invalid search')
        self.assertEqual(self.session(dict(msg, query_index=2))[0]['error'], 'invalid vector data or index')
        self.assertEqual(self.session(dict(msg, command='arbitrary'))[0]['error'], 'invalid search')
        response = self.session(msg)[0]
        self.assertEqual(response['results'], [[1.0, 0], [0.5, 1]])
        self.assertEqual(response['sha256'], __import__('hashlib').sha256(raw).hexdigest())
        self.store.search_slot.acquire()
        try: self.assertEqual(self.session(msg)[0]['error'], 'search busy')
        finally: self.store.search_slot.release()
        self.assertEqual(self.store.items['v']['readers'], 0)

    def test_search_rejects_nonfinite_and_bad_dimensions(self):
        self.store = xram.Store(4096, 30, enable_search=True)
        bad = struct.pack('<256f', *([float('nan')] + [0.0]*255))
        self.session(self.req('put', key='nan', size=len(bad)), bad)
        self.session(self.req('put', key='short', size=1), b'x')
        for key in ('nan', 'short'):
            self.assertEqual(self.session(self.req('search', key=key, query_index=0, top=1))[0]['error'], 'invalid vector data or index')
            self.assertEqual(self.store.items[key]['readers'], 0)

    def test_search_retains_deleted_data_until_compute_finishes(self):
        from unittest.mock import patch
        self.store = xram.Store(4096, 30, enable_search=True)
        data = struct.pack('<256f', *([1.0] + [0.0]*255))
        self.session(self.req('put', key='v', size=len(data)), data)
        entered, release = threading.Event(), threading.Event()
        original = xram.heapq.nlargest
        def delayed(*args, **kwargs):
            entered.set()
            if not release.wait(3): raise RuntimeError('test timeout')
            return original(*args, **kwargs)
        result = []
        with patch('xram.heapq.nlargest', side_effect=delayed):
            thread = threading.Thread(target=lambda: result.append(self.session(self.req('search', key='v', query_index=0, top=1))[0]))
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                self.assertTrue(self.session(self.req('delete', key='v'))[0]['ok'])
                self.assertEqual(self.store.used, len(data))
            finally:
                release.set(); thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result[0]['ok'])
        self.assertEqual(self.store.used, 0)

    def test_authenticated_cancellation_of_active_search(self):
        from unittest.mock import patch
        self.store = xram.Store(4096, 30, enable_search=True, search_queue=1)
        data = struct.pack('<256f', *([1.0] + [0.0]*255))
        self.session(self.req('put', key='v', size=len(data)), data)
        entered, release = threading.Event(), threading.Event()
        original = xram.heapq.nlargest
        def delayed(*args, **kwargs):
            entered.set()
            if not release.wait(3): raise RuntimeError('test timeout')
            return original(*args, **kwargs)
        result=[]; request_id='c'*32
        with patch('xram.heapq.nlargest', side_effect=delayed):
            thread=threading.Thread(target=lambda:result.append(self.session(self.req('search',key='v',query_index=0,top=1,request_id=request_id))[0]));thread.start()
            try:
                self.assertTrue(entered.wait(2))
                self.assertEqual(self.session(dict(op='cancel_search',token='b'*64,request_id=request_id))[0]['error'],'unauthorized')
                self.assertTrue(self.session(self.req('cancel_search',request_id=request_id))[0]['canceled'])
            finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(result[0]['error'],'search canceled')
        self.assertFalse(self.store.scheduler.tickets);self.assertEqual(self.store.items['v']['readers'],0)

    def test_peer_allowlist_preserves_default_and_single_peer(self):
        self.assertEqual(xram.peer_allowlist(None), {'127.0.0.1'})
        self.assertEqual(xram.peer_allowlist('192.0.2.21'), {'192.0.2.21'})
        self.assertEqual(xram.peer_allowlist(['127.0.0.1','192.0.2.134']), {'127.0.0.1','192.0.2.134'})

    def test_persistent_requests_reauthenticate_and_legacy_closes(self):
        client, server = socket.socketpair()
        def work():
            with server:
                while self.store.serve(server, TOKEN): pass
        thread = threading.Thread(target=work); thread.start()
        with client:
            xram.send_msg(client, self.req('ping', keep=True))
            self.assertTrue(xram.recv_msg(client)['ok'])
            xram.send_msg(client, self.req('ping', keep=True))
            self.assertTrue(xram.recv_msg(client)['ok'])
            xram.send_msg(client, dict(op='ping', token='b'*64, keep=True))
            self.assertEqual(xram.recv_msg(client)['error'], 'unauthorized')
            self.assertEqual(client.recv(1), b'')
        thread.join(2); self.assertFalse(thread.is_alive())

        client, server = socket.socketpair()
        thread = threading.Thread(target=lambda: (self.store.serve(server,TOKEN), server.close())); thread.start()
        with client:
            xram.send_msg(client,self.req('ping'))
            self.assertTrue(xram.recv_msg(client)['ok'])
            self.assertEqual(client.recv(1),b'')
        thread.join(2); self.assertFalse(thread.is_alive())

    def session(self, msg, payload=None, abort=False):
        client, server = socket.socketpair()
        errors = []
        def work():
            with server:
                try: self.store.serve(server, TOKEN)
                except (EOFError, OSError) as e: errors.append(e)
        thread = threading.Thread(target=work); thread.start()
        with client:
            xram.send_msg(client, msg)
            result = xram.recv_msg(client)
            if result.get('ready'):
                if payload: client.sendall(payload)
                if abort: client.shutdown(socket.SHUT_RDWR)
                else: result = xram.recv_msg(client)
            elif msg['op'] == 'get' and result.get('ok'):
                result['data'] = bytes(xram.read_exact(client, result['size']))
        thread.join(2)
        self.assertFalse(thread.is_alive())
        return result, errors

    def req(self, op, **kw): return dict(op=op, token=TOKEN, **kw)

    def test_round_trip_capacity_delete(self):
        self.assertTrue(self.session(self.req('put', key='a', size=8), b'12345678')[0]['ok'])
        self.assertEqual(self.session(self.req('get', key='a'))[0]['data'], b'12345678')
        self.assertEqual(self.session(self.req('put', key='b', size=1))[0]['error'], 'capacity exceeded')
        self.assertEqual(self.session(self.req('put', key='a', size=1))[0]['error'], 'key exists; delete first')
        self.assertTrue(self.session(self.req('delete', key='a'))[0]['ok'])
        self.assertEqual(self.store.used, 0)

    def test_unauthorized_and_invalid(self):
        self.assertEqual(self.session(dict(op='ping', token='\u00e9'))[0]['error'], 'unauthorized')
        self.assertEqual(self.session(self.req('put', key='x', size=9))[0]['error'], 'invalid size')
        self.assertEqual(self.session(self.req('get', key='missing'))[0]['error'], 'not found')

    def test_incomplete_upload_does_not_consume_capacity(self):
        _, errors = self.session(self.req('put', key='x', size=8), b'abc', abort=True)
        self.assertTrue(errors)
        self.assertEqual(self.store.used, 0)
        self.assertNotIn('x', self.store.items)
        self.assertTrue(self.session(self.req('put', key='x', size=8), b'12345678')[0]['ok'])

    def test_expiry(self):
        self.session(self.req('put', key='x', size=1), b'a')
        time.sleep(1.05)
        self.assertEqual(self.session(self.req('get', key='x'))[0]['error'], 'not found')
        self.assertEqual(self.store.used, 0)

    def test_bad_headers(self):
        a, b = socket.socketpair()
        with a, b:
            a.sendall(b'\x00\x00\x20\x00')
            with self.assertRaises(ValueError): xram.recv_msg(b)

    def test_read_deadline_bounds_partial_headers(self):
        a,b=socket.socketpair()
        with a,b:
            a.sendall(b'x')
            with self.assertRaises(TimeoutError): xram.read_exact(b, 2, time.monotonic()+0.05)

    def test_lru_eviction_and_stats(self):
        self.session(self.req('put', key='a', size=3), b'aaa')
        self.session(self.req('put', key='b', size=3), b'bbb')
        self.session(self.req('get', key='a'))
        self.session(self.req('put', key='c', size=3, evict=True), b'ccc')
        self.assertEqual(self.session(self.req('get', key='b'))[0]['error'], 'not found')
        self.assertEqual(self.session(self.req('get', key='a'))[0]['data'], b'aaa')
        stats = self.session(self.req('stats'))[0]
        self.assertEqual((stats['evictions'], stats['hits'], stats['misses'], stats['used']), (1, 2, 1, 6))

    def test_concurrent_reservation_bounds_and_cleanup(self):
        client, server = socket.socketpair()
        errors = []
        def work():
            with server:
                try: self.store.serve(server, TOKEN)
                except (EOFError, OSError) as e: errors.append(e)
        thread = threading.Thread(target=work); thread.start()
        with client:
            xram.send_msg(client, self.req('put', key='one', size=8, evict=True))
            self.assertTrue(xram.recv_msg(client)['ready'])
            self.assertEqual(self.store.snapshot()['reserved'], 8)
            self.assertEqual(self.session(self.req('put', key='two', size=1, evict=True))[0]['error'], 'capacity exceeded')
            self.assertEqual(self.session(self.req('put', key='one', size=1, evict=True))[0]['error'], 'key exists; delete first')
            client.sendall(b'partial')
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(errors)
        self.assertEqual((self.store.snapshot()['used'], self.store.snapshot()['reserved']), (0, 0))

    def test_individual_ttl_validation(self):
        self.assertEqual(self.session(self.req('put', key='x', size=1, ttl=0))[0]['error'], 'invalid size')
        self.assertTrue(self.session(self.req('put', key='x', size=1, ttl=1), b'x')[0]['ok'])
        time.sleep(1.05)
        self.store.expire()
        self.assertEqual(self.store.snapshot()['expirations'], 1)

    def test_active_read_remains_accounted_after_delete(self):
        self.session(self.req('put', key='pinned', size=8), b'12345678')
        entered, release = threading.Event(), threading.Event()
        payload = json.dumps(self.req('get', key='pinned')).encode()
        class SlowConnection:
            def __init__(self): self.input = bytearray(struct.pack('!I', len(payload)) + payload); self.sent = 0
            def recv_into(self, view):
                n = min(len(view), len(self.input)); view[:n] = self.input[:n]; del self.input[:n]; return n
            def sendall(self, data):
                self.sent += 1
                if self.sent == 2:
                    entered.set()
                    if not release.wait(2): raise TimeoutError('reader did not release')
        thread = threading.Thread(target=self.store.serve, args=(SlowConnection(), TOKEN))
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertTrue(self.store.delete('pinned'))
            self.assertEqual(self.store.snapshot()['used'], 8)
            self.assertEqual(self.session(self.req('put', key='new', size=1, evict=True))[0]['error'], 'capacity exceeded')
        finally:
            release.set(); thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.store.snapshot()['used'], 0)

if __name__ == '__main__': unittest.main()
