"""Real loopback TLS smoke test; skips only when OpenSSL is unavailable."""
import os
from pathlib import Path
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import xram

OPENSSL=shutil.which('openssl')

@unittest.skipUnless(OPENSSL, 'OpenSSL is required to create an ephemeral test certificate')
class LoopbackIntegrationTests(unittest.TestCase):
    def test_cache_and_bounded_search_on_loopback(self):
        with tempfile.TemporaryDirectory(prefix='xram-loopback-') as directory:
            root=Path(directory);token=secrets.token_hex(32);(root/'token').write_text(token,encoding='ascii')
            command=[OPENSSL,'req','-x509','-newkey','rsa:2048','-sha256','-nodes','-days','1',
                     '-keyout',str(root/'key.pem'),'-out',str(root/'cert.pem'),'-subj','/CN=127.0.0.1',
                     '-addext','subjectAltName=IP:127.0.0.1']
            subprocess.run(command,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=20)
            with socket.socket() as probe:
                probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
            args=[sys.executable,str(Path(__file__).with_name('xram.py')),'server','--port',str(port),
                  '--token',str(root/'token'),'--cert',str(root/'cert.pem'),'--key',str(root/'key.pem'),
                  '--limit','4096','--enable-search']
            flags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0
            server=subprocess.Popen(args,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,creationflags=flags)
            try:
                client=xram.CacheClient('127.0.0.1',port,str(root/'cert.pem'),token);deadline=time.monotonic()+8
                while True:
                    if server.poll() is not None: self.fail('loopback server exited before readiness')
                    try:client.request('ping');break
                    except (ConnectionError,TimeoutError,OSError):
                        if time.monotonic()>=deadline:raise
                        time.sleep(.05)
                vector=struct.pack('<256f',*([1.0]+[0.0]*255));client.put('fixture',vector)
                self.assertEqual(client.get('fixture'),vector)
                with client.connect() as conn:
                    xram.send_msg(conn,dict(op='search',token=token,key='fixture',query_index=0,top=1))
                    result=xram.recv_msg(conn)
                self.assertEqual(result['results'],[[1.0,0]])
                self.assertTrue(client.delete('fixture')['ok'])
                self.assertIsNone(client.get('fixture'))
            finally:
                server.terminate()
                try:server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server.kill();server.wait(timeout=5)

if __name__=='__main__': unittest.main()
