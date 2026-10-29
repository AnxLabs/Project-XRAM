"""Local-LAN benchmark using only generated, non-sensitive test data."""
import argparse, concurrent.futures, hashlib, json, math, os, random, statistics, struct, sys, tempfile, threading, time, uuid, zlib
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import xram, compute_near_memory as near

def process_memory():
    if os.name!='nt': return {'note':'Windows process memory unavailable on this platform'}
    import ctypes
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_=[('cb',wintypes.DWORD),('faults',wintypes.DWORD)]+[(name,ctypes.c_size_t) for name in ('peak_working_set','working_set','peak_paged_pool','paged_pool','peak_nonpaged_pool','nonpaged_pool','pagefile','peak_pagefile','private')]
    kernel=ctypes.WinDLL('kernel32',use_last_error=True);psapi=ctypes.WinDLL('psapi',use_last_error=True);kernel.GetCurrentProcess.restype=wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD];psapi.GetProcessMemoryInfo.restype=wintypes.BOOL
    v=Counters();v.cb=ctypes.sizeof(v)
    if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(),ctypes.byref(v),v.cb):raise ctypes.WinError()
    return {'working_set':v.working_set,'peak_working_set':v.peak_working_set,'private_bytes':v.private,'page_faults':v.faults}

def summary(values):
    a=sorted(values);return {'n':len(a),'median_ms':statistics.median(a),'p95_ms':a[math.ceil(.95*len(a))-1], 'samples_ms':values}

def vector_score(data,index=0):
    query=struct.unpack_from('<256f',data,index*1024)
    return sorted(((sum(a*b for a,b in zip(row,query)),i) for i,row in enumerate(struct.iter_unpack('<256f',data))),reverse=True)[:5]

def run(a,token):
    windows_before=process_memory()
    r=random.Random(728); payload=r.randbytes(a.mib*1048576); digest=hashlib.sha256(payload).hexdigest()
    vgen=random.Random(91);vectors=b''.join(struct.pack('<256f',*(vgen.random() for _ in range(256))) for _ in range(1024));vhash=hashlib.sha256(vectors).hexdigest();reference=vector_score(vectors)
    compressible=(b'XRAM benchmark fixture 0123456789\n'*((len(payload)//33)+1))[:len(payload)]
    client=xram.CacheClient(a.host,a.port,a.cert,token); prefix='public-bench-'+uuid.uuid4().hex; inserted=[]
    results={'fixture':'Python PRNG seed 728; synthetic uniform bytes; vectors seed 91; no source documents','payload_bytes':len(payload),'payload_sha256':digest,'vector_bytes':len(vectors),'vector_sha256':vhash,'samples':a.samples,'scenarios':{}}
    def measure(name,fn):
        times=[]
        for _ in range(a.samples):
            t=time.perf_counter_ns();fn();times.append((time.perf_counter_ns()-t)/1e6)
        results['scenarios'][name]=summary(times)
    try:
        results['server_before']=client.stats();key=prefix+'-data'; vector_key=prefix+'-vectors'
        client.put(key,payload,ttl=600);inserted.append(key);client.put(vector_key,vectors,ttl=600);inserted.append(vector_key)
        results['server_seeded']=client.stats()
        measure('tls_ping',lambda:client.request('ping'))
        def read():
            got=client.get(key)
            if got is None or hashlib.sha256(got).hexdigest()!=digest:raise ValueError('remote integrity error')
        measure('remote_read',read)
        def put():
            name=prefix+'-'+uuid.uuid4().hex
            try:client.request('put',name,payload,ttl=600)
            finally:client.delete(name)
        measure('remote_write_including_delete',put)
        def local_ram():
            v=bytes(memoryview(payload))
            if hashlib.sha256(v).hexdigest()!=digest:raise ValueError('RAM integrity error')
        measure('local_ram_copy_hash',local_ram)
        with tempfile.TemporaryDirectory(prefix='xram-public-',dir=a.scratch) as folder:
            file=Path(folder)/'payload.bin';file.write_bytes(payload)
            def disk():
                b=file.read_bytes()
                if hashlib.sha256(b).hexdigest()!=digest:raise ValueError('disk integrity error')
            measure('warm_ssd_read_hash',disk)
        for label,source in [('random',payload),('repetitive',compressible)]:
            packed=zlib.compress(source,level=1);results['scenarios']['compression_'+label]={'compressed_bytes':len(packed),'ratio':len(packed)/len(source)}
            measure('compress_'+label,lambda:zlib.compress(source,level=1))
            measure('decompress_'+label,lambda:zlib.decompress(packed))
        measure('local_vector_score',lambda:vector_score(vectors))
        with near.Session(client) as session:
            def search():
                got=session.search(vector_key,0,5,vhash)
                if any(a[1]!=b[1] or not math.isclose(a[0],b[0],abs_tol=1e-12) for a,b in zip(got,reference)):raise ValueError('search mismatch')
            measure('remote_vector_search',search)
            results['remote_search_application_bytes']={'sent':session.uploaded,'received':session.downloaded}
        def concurrency(count):
            barrier=threading.Barrier(count)
            def worker(i):
                out=[]
                with near.Session(client) as s:
                    barrier.wait(timeout=10)
                    for _ in range(a.concurrent_rounds):
                        start=time.perf_counter_ns()
                        try:
                            got=s.search(vector_key,0,5,vhash)
                            if len(got)!=len(reference) or any(x[1]!=y[1] or not math.isclose(x[0],y[0],abs_tol=1e-12) for x,y in zip(got,reference)):raise ValueError('incorrect result')
                            status='ok'
                        except xram.CacheError as e:status=str(e)
                        except (OSError,EOFError,ValueError) as e:status=type(e).__name__
                        out.append({'status':status,'ms':(time.perf_counter_ns()-start)/1e6})
                    return {'out':out,'sent':s.uploaded,'received':s.downloaded}
            t=time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:work=list(pool.map(worker,range(count)))
            wall=time.perf_counter()-t;out=[o for x in work for o in x['out']]; counts={}
            for o in out:counts[o['status']]=counts.get(o['status'],0)+1
            return {'attempted':len(out),'counts':counts,'wall_seconds':wall,'completed_per_second':counts.get('ok',0)/wall,'all_response_latency':summary([o['ms'] for o in out]),'outcomes':out,'application_bytes_sent':sum(x['sent'] for x in work),'application_bytes_received':sum(x['received'] for x in work)}
        results['concurrency']={str(count):concurrency(count) for count in (1,4,8)}
        results['server_after']=client.stats();results['windows_before']=windows_before;results['windows_after']=process_memory()
        return results
    finally:
        for name in inserted:
            try:client.delete(name)
            except (OSError,xram.CacheError):pass

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--host',required=True);p.add_argument('--port',type=int,default=39471);p.add_argument('--cert',required=True)
    p.add_argument('--token-file',help='omit to read from stdin');p.add_argument('--scratch',required=True);p.add_argument('--mib',type=int,default=8);p.add_argument('--samples',type=int,default=10);p.add_argument('--concurrent-rounds',type=int,default=5);a=p.parse_args()
    if not 1<=a.mib<=16 or not 5<=a.samples<=100 or not 1<=a.concurrent_rounds<=20:p.error('invalid resource limit')
    token=xram.token_from(a.token_file) if a.token_file else sys.stdin.readline().strip();print(json.dumps(run(a,token),indent=2))
