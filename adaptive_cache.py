"""Small, measured cache selector for trusted, versioned byte artifacts.

No background work, remote discovery or automatic disk writes. Measurements are
process-local and scoped to the exact artifact, backend and concurrency level.
"""
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
import errno
import hashlib
import hmac
import math
from pathlib import Path
import ssl
import statistics
import threading
import time

import xram
import xram_codec

TIERS = ('ram', 'disk', 'xram', 'recompute')

@dataclass(frozen=True)
class Artifact:
    key: str
    size: int
    sha256: str
    scope: str
    remote_key: str | None = None
    remote_size: int | None = None
    remote_sha256: str | None = None

    def __post_init__(self):
        if not isinstance(self.key,str) or not self.key or len(self.key)>128:
            raise ValueError('invalid artifact key')
        if type(self.size) is not int or not 0 <= self.size <= xram.MAX_BYTES:
            raise ValueError('invalid artifact size')
        def valid_digest(value):
            return isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value)
        if not valid_digest(self.sha256): raise ValueError('invalid artifact digest')
        if not isinstance(self.scope,str) or not self.scope.isascii() or not 1<=len(self.scope)<=128:
            raise ValueError('invalid measurement scope')
        if self.remote_key is not None:
            if not isinstance(self.remote_key,str) or not self.remote_key.isascii() or not 1<=len(self.remote_key)<=128:
                raise ValueError('invalid remote key')
            if type(self.remote_size) is not int or not 0 <= self.remote_size <= xram.MAX_BYTES or not valid_digest(self.remote_sha256):
                raise ValueError('invalid remote artifact metadata')

@dataclass
class Result:
    data: bytes
    selected: str
    attempted: tuple
    selection_ns: int

class Measurements:
    """Recent successful latencies and misses; never persist artifact contents."""
    def __init__(self, *, min_samples=3, ttl=300, window=32, max_series=512, clock=time.monotonic):
        if (type(min_samples) is not int or type(window) is not int or type(max_series) is not int or
            min_samples<1 or window<min_samples or max_series<1 or
            not isinstance(ttl,(int,float)) or not math.isfinite(ttl) or ttl<=0):
            raise ValueError('invalid measurement policy')
        self.min_samples,self.ttl,self.window,self.clock=min_samples,ttl,window,clock
        self.max_series=max_series
        self.samples=OrderedDict(); self.counts=defaultdict(int)
        self.lock=threading.RLock()

    def record(self, spec, tier, concurrency, duration_ns, outcome):
        if tier not in TIERS or type(concurrency) is not int or concurrency<1 or type(duration_ns) is not int or duration_ns<0:
            raise ValueError('invalid measurement')
        if outcome not in ('hit','miss','unavailable'): raise ValueError('invalid outcome')
        key=(spec.scope,spec.sha256,spec.size,tier,concurrency)
        with self.lock:
            if key not in self.samples:
                if len(self.samples)>=self.max_series:
                    expired,_=self.samples.popitem(last=False)
                    for kind in ('hit','miss','unavailable'): self.counts.pop((expired,kind),None)
                self.samples[key]=deque(maxlen=self.window)
            self.samples.move_to_end(key)
            self.counts[(key,outcome)]+=1
            if outcome=='hit': self.samples[key].append((self.clock(),duration_ns))

    def estimate(self, spec, tier, concurrency):
        now=self.clock(); key=(spec.scope,spec.sha256,spec.size,tier,concurrency)
        with self.lock:
            rows=[n for at,n in self.samples.get(key,()) if 0<=now-at<=self.ttl]
        return statistics.median(rows) if len(rows)>=self.min_samples else None

    def counters(self):
        with self.lock:
            return {'hits':sum(v for (_,kind),v in self.counts.items() if kind=='hit'),
                    'misses':sum(v for (_,kind),v in self.counts.items() if kind=='miss'),
                    'unavailable':sum(v for (_,kind),v in self.counts.items() if kind=='unavailable')}

class AdaptiveCache:
    def __init__(self, measurements, *, remote=None, ram_limit=32*1024*1024,
                 remote_margin=0.10, cooldown=5, clock=time.monotonic):
        if (type(ram_limit) is not int or ram_limit<0 or not isinstance(remote_margin,(int,float)) or
            not math.isfinite(remote_margin) or not 0<=remote_margin<1 or
            not isinstance(cooldown,(int,float)) or not math.isfinite(cooldown) or cooldown<0):
            raise ValueError('invalid cache policy')
        self.measurements,self.remote,self.ram_limit=measurements,remote,ram_limit
        self.margin,self.cooldown,self.clock=remote_margin,cooldown,clock
        self.ram=OrderedDict(); self.ram_bytes=0; self.remote_until=0
        self.lock=threading.RLock()

    @staticmethod
    def _validate(spec, data):
        if len(data)!=spec.size or not hmac.compare_digest(hashlib.sha256(data).hexdigest(),spec.sha256):
            raise ValueError('artifact integrity failure')
        return bytes(data)

    def put_ram(self, spec, data):
        data=self._validate(spec,data)
        if len(data)>self.ram_limit: return False
        identity=(spec.key,spec.sha256)
        with self.lock:
            old=self.ram.pop(identity,None)
            if old is not None: self.ram_bytes-=len(old)
            while self.ram and self.ram_bytes+len(data)>self.ram_limit:
                _,evicted=self.ram.popitem(last=False); self.ram_bytes-=len(evicted)
            self.ram[identity]=data; self.ram_bytes+=len(data)
        return True

    def _ram_get(self,spec):
        identity=(spec.key,spec.sha256)
        with self.lock:
            data=self.ram.get(identity)
            if data is not None: self.ram.move_to_end(identity)
            return data

    def _plan(self,spec,disk,concurrency,allow_remote):
        with self.lock: remote_ready=self.clock()>=self.remote_until
        remote_cost=(self.measurements.estimate(spec,'xram',concurrency)
                     if allow_remote and self.remote is not None and spec.remote_key and remote_ready else None)
        def insert_remote(ordered, estimates):
            if remote_cost is not None:
                # Unknown choices stay ahead; remote precedes only measured,
                # meaningfully slower candidates. It remains an available
                # fallback if a faster local object disappears during lookup.
                for index,tier in enumerate(ordered):
                    cost=estimates.get(tier)
                    if cost is not None and remote_cost < cost*(1-self.margin):
                        ordered.insert(index,'xram'); break
            return ordered
        available=[]
        ram_present=self._ram_get(spec) is not None
        if ram_present:
            # Avoid a filesystem probe when fresh measurements already establish
            # that resident RAM wins over every eligible measured alternative.
            ram_cost=self.measurements.estimate(spec,'ram',concurrency)
            compute_cost=self.measurements.estimate(spec,'recompute',concurrency)
            disk_cost=self.measurements.estimate(spec,'disk',concurrency) if disk is not None else None
            if (ram_cost is not None and compute_cost is not None and ram_cost<=compute_cost and
                (disk is None or (disk_cost is not None and ram_cost<=disk_cost)) and
                (remote_cost is None or remote_cost>=ram_cost*(1-self.margin))):
                return insert_remote(['ram'] + (['disk'] if disk is not None else []) + ['recompute'],
                                     {'ram':ram_cost,'disk':disk_cost,'recompute':compute_cost})
            available.append('ram')
        if disk is not None and Path(disk).is_file(): available.append('disk')
        available.append('recompute')
        estimates={tier:self.measurements.estimate(spec,tier,concurrency) for tier in available}
        # Unknown local paths are preferred without paying for a speculative remote probe.
        unknown_local=[t for t in available if t in ('ram','disk') and estimates[t] is None]
        if unknown_local: ordered=unknown_local + [t for t in available if t not in unknown_local]
        else: ordered=sorted(available,key=lambda t:(estimates[t] is None, estimates[t] if estimates[t] is not None else 0, TIERS.index(t)))
        return insert_remote(ordered,estimates)

    def _remote_failure(self):
        with self.lock: self.remote_until=max(self.remote_until,self.clock()+self.cooldown)

    def get(self, spec, *, compute, disk=None, concurrency=1, strategy='adaptive', promote=False, allow_remote=True):
        if type(concurrency) is not int or concurrency<1: raise ValueError('invalid concurrency')
        if strategy not in ('adaptive',)+TIERS: raise ValueError('invalid strategy')
        t=time.perf_counter_ns()
        if strategy=='adaptive': plan=self._plan(spec,disk,concurrency,allow_remote)
        else: plan=[strategy]+([] if strategy=='recompute' else ['recompute'])
        selection_ns=time.perf_counter_ns()-t
        attempted=[]
        for tier in plan:
            attempted.append(tier); start=time.perf_counter_ns()
            try:
                if tier=='ram': data=self._ram_get(spec)
                elif tier=='disk':
                    if disk is None: data=None
                    else:
                        try:
                            with open(disk,'rb') as f: data=f.read(spec.size+1)
                        except FileNotFoundError: data=None
                elif tier=='xram':
                    if self.remote is None or spec.remote_key is None or not allow_remote: data=None
                    else:
                        packed=self.remote.get(spec.remote_key)
                        if packed is None: data=None
                        else:
                            if len(packed)!=spec.remote_size or not hmac.compare_digest(hashlib.sha256(packed).hexdigest(),spec.remote_sha256):
                                raise ValueError('compressed artifact integrity failure')
                            data=xram_codec.decompress(packed,size=spec.size,sha256=spec.sha256)
                else: data=compute()
                if data is None:
                    self.measurements.record(spec,tier,concurrency,time.perf_counter_ns()-start,'miss')
                    if tier=='xram': self._remote_failure()
                    continue
                data=self._validate(spec,data)
            except ssl.SSLError: raise
            except (ConnectionError, TimeoutError, EOFError):
                if tier!='xram': raise
                self.measurements.record(spec,tier,concurrency,time.perf_counter_ns()-start,'unavailable')
                self._remote_failure(); continue
            except OSError as exc:
                transient={errno.ECONNREFUSED,errno.ECONNRESET,errno.ECONNABORTED,errno.ETIMEDOUT,
                           errno.ENETUNREACH,errno.EHOSTUNREACH,errno.EPIPE}
                windows={10050,10051,10053,10054,10060,10061,10064,10065}
                if tier!='xram' or (exc.errno not in transient and getattr(exc,'winerror',None) not in windows): raise
                self.measurements.record(spec,tier,concurrency,time.perf_counter_ns()-start,'unavailable')
                self._remote_failure(); continue
            self.measurements.record(spec,tier,concurrency,time.perf_counter_ns()-start,'hit')
            if promote and tier!='ram': self.put_ram(spec,data)
            return Result(data,tier,tuple(attempted),selection_ns)
        raise RuntimeError('no usable artifact source')
