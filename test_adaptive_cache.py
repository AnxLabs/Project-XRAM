import concurrent.futures
import hashlib
import ssl
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import xram
import xram_codec
from adaptive_cache import AdaptiveCache, Artifact, Measurements

DATA=b'test data'*128
PACKED=xram_codec.compress(DATA)
def spec(data=DATA,packed=PACKED,key='artifact'):
    return Artifact(key,len(data),hashlib.sha256(data).hexdigest(),'test-scope','remote',len(packed),hashlib.sha256(packed).hexdigest())
class Remote:
    def __init__(self): self.result=PACKED; self.calls=0
    def get(self,key):
        self.calls+=1
        if isinstance(self.result,Exception): raise self.result
        return self.result

class Tests(unittest.TestCase):
    def setUp(self):
        self.s=spec(); self.remote=Remote(); self.book=Measurements(min_samples=3)
        self.cache=AdaptiveCache(self.book,remote=self.remote,ram_limit=4096)
    def train(self,tier,ms,concurrency=1):
        for _ in range(3): self.book.record(self.s,tier,concurrency,ms*1000000,'hit')
    def test_chooses_local_and_remote_only_when_measured_better(self):
        self.train('recompute',100); self.train('xram',20); self.train('ram',1)
        self.cache.put_ram(self.s,DATA)
        r=self.cache.get(self.s,compute=lambda:DATA)
        self.assertEqual(r.selected,'ram'); self.assertEqual(self.remote.calls,0)
        other=AdaptiveCache(self.book,remote=self.remote)
        r=other.get(self.s,compute=lambda:DATA)
        self.assertEqual(r.selected,'xram'); self.assertEqual(r.data,DATA)
    def test_measured_ram_fast_path_skips_disk_probe(self):
        for tier,ms in [('ram',1),('disk',5),('xram',20),('recompute',100)]: self.train(tier,ms)
        self.cache.put_ram(self.s,DATA)
        with mock.patch.object(Path,'is_file',side_effect=AssertionError('unexpected disk probe')):
            result=self.cache.get(self.s,compute=lambda:DATA,disk='unused')
        self.assertEqual(result.selected,'ram'); self.assertEqual(self.remote.calls,0)
    def test_measured_remote_can_beat_a_slow_resident_entry(self):
        for tier,ms in [('ram',100),('disk',150),('xram',10),('recompute',200)]: self.train(tier,ms)
        self.cache.put_ram(self.s,DATA)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'artifact'; p.write_bytes(DATA)
            result=self.cache.get(self.s,compute=lambda:DATA,disk=p)
        self.assertEqual(result.selected,'xram'); self.assertEqual(result.data,DATA)
    def test_disappearing_local_entry_uses_measured_remote_fallback(self):
        for tier,ms in [('ram',1),('disk',5),('xram',20),('recompute',100)]: self.train(tier,ms)
        with mock.patch.object(self.cache,'_ram_get',side_effect=[DATA,None]):
            result=self.cache.get(self.s,compute=lambda:DATA,disk='missing-file')
        self.assertEqual(result.selected,'xram')
        self.assertEqual(result.attempted,('ram','disk','xram'))
    def test_unknown_or_stale_measurements_do_not_probe_remote(self):
        r=self.cache.get(self.s,compute=lambda:DATA)
        self.assertEqual(r.selected,'recompute'); self.assertEqual(self.remote.calls,0)
        clock=[0]; book=Measurements(ttl=1,clock=lambda:clock[0])
        for tier,cost in [('xram',1),('recompute',10)]:
            for _ in range(3): book.record(self.s,tier,1,cost,'hit')
        clock[0]=2; c=AdaptiveCache(book,remote=self.remote,clock=lambda:clock[0])
        self.assertEqual(c.get(self.s,compute=lambda:DATA).selected,'recompute')
        self.assertEqual(self.remote.calls,0)
    def test_margin_and_concurrency_are_scoped(self):
        self.train('recompute',100); self.train('xram',95)
        self.assertEqual(self.cache.get(self.s,compute=lambda:DATA).selected,'recompute')
        self.assertEqual(self.cache.get(self.s,compute=lambda:DATA,concurrency=4).selected,'recompute')
        other=Artifact(self.s.key,self.s.size,self.s.sha256,'different-disk-and-node',self.s.remote_key,self.s.remote_size,self.s.remote_sha256)
        self.assertIsNone(self.book.estimate(other,'recompute',1))
        self.assertIsNone(self.book.estimate(other,'xram',1))
    def test_miss_and_outage_fall_back_and_cool_down(self):
        self.train('recompute',100); self.train('xram',20)
        self.remote.result=None
        r=self.cache.get(self.s,compute=lambda:DATA)
        self.assertEqual((r.selected,r.attempted),('recompute',('xram','recompute')))
        self.assertEqual(self.cache.get(self.s,compute=lambda:DATA).selected,'recompute')
        self.assertEqual(self.remote.calls,1)
        c=AdaptiveCache(self.book,remote=self.remote,cooldown=0); self.remote.result=ConnectionResetError()
        self.assertEqual(c.get(self.s,compute=lambda:DATA).selected,'recompute')
        self.assertGreaterEqual(self.book.counters()['unavailable'],1)
    def test_corruption_and_authentication_fail_closed(self):
        self.train('recompute',100); self.train('xram',20)
        self.remote.result=b'bad'
        with self.assertRaises(ValueError): self.cache.get(self.s,compute=lambda:DATA)
        self.remote.result=ssl.SSLError('bad certificate')
        with self.assertRaises(ssl.SSLError): self.cache.get(self.s,compute=lambda:DATA)
        self.remote.result=xram.CacheError('unauthorized')
        with self.assertRaises(xram.CacheError): self.cache.get(self.s,compute=lambda:DATA)
    def test_disk_ram_limit_and_version_invalidation(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'artifact'; p.write_bytes(DATA)
            self.assertEqual(self.cache.get(self.s,compute=lambda:DATA,disk=p).selected,'disk')
            p.write_bytes(b'corrupted')
            with self.assertRaises(ValueError): self.cache.get(self.s,compute=lambda:DATA,disk=p,strategy='disk')
        c=AdaptiveCache(self.book,ram_limit=len(DATA)-1)
        self.assertFalse(c.put_ram(self.s,DATA))
        self.cache.put_ram(self.s,DATA)
        changed=b'another'; changed_spec=Artifact('artifact',len(changed),hashlib.sha256(changed).hexdigest(),'test-scope')
        self.assertEqual(self.cache.get(changed_spec,compute=lambda:changed).selected,'recompute')
    def test_concurrent_ram_reads_and_accounting(self):
        self.cache.put_ram(self.s,DATA)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            rows=list(pool.map(lambda _:self.cache.get(self.s,compute=lambda:DATA),range(80)))
        self.assertTrue(all(r.selected=='ram' and r.data==DATA for r in rows))
        self.assertEqual(self.cache.ram_bytes,len(DATA))
    def test_measurement_series_are_bounded(self):
        book=Measurements(min_samples=1,max_series=2)
        for data in (b'a',b'b',b'c'):
            item=Artifact('key',len(data),hashlib.sha256(data).hexdigest(),'test-scope')
            book.record(item,'recompute',1,100,'hit')
        self.assertEqual(len(book.samples),2)
        self.assertEqual(book.counters()['hits'],2)
    def test_invalid_measurements_and_policy_are_rejected(self):
        with self.assertRaises(ValueError): Measurements(ttl=float('nan'))
        with self.assertRaises(ValueError): Measurements(max_series=0)
        with self.assertRaises(ValueError): self.book.record(self.s,'ram',1,float('nan'),'hit')
        with self.assertRaises(ValueError): AdaptiveCache(self.book,cooldown=float('nan'))

if __name__=='__main__': unittest.main()
