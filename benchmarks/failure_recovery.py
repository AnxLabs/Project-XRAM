"""Three-stage failure test for a disposable server; never use real data."""
import argparse,hashlib,json,random,ssl,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]));import xram
p=argparse.ArgumentParser();p.add_argument('phase',choices=['seed','offline','recover']);p.add_argument('--host',required=True);p.add_argument('--port',type=int,default=39471);p.add_argument('--cert',required=True);p.add_argument('--token-file');p.add_argument('--key',required=True);a=p.parse_args()
token=xram.token_from(a.token_file) if a.token_file else sys.stdin.readline().strip();c=xram.CacheClient(a.host,a.port,a.cert,token)
data=random.Random(117).randbytes(65536);digest=hashlib.sha256(data).hexdigest();t=time.perf_counter()
if a.phase=='seed':
    c.put(a.key,data,ttl=600); result={'seeded':True,'sha256':digest}
elif a.phase=='offline':
    try:
        c.get(a.key);raise RuntimeError('server was expected to be stopped')
    except ssl.SSLError:raise
    except (ConnectionError,TimeoutError,EOFError) as e:failure=type(e).__name__
    except OSError as e:
        if e.errno not in (10061,10060,111,110):raise
        failure=type(e).__name__
    fallback=random.Random(117).randbytes(65536)
    result={'failure':failure,'fallback_sha256_verified':hashlib.sha256(fallback).hexdigest()==digest}
else:
    missing=c.get(a.key) is None
    if not missing:raise RuntimeError('server retained old data; expected restart')
    c.put(a.key,data,ttl=600)
    try:result={'old_data_missing':missing,'republished_sha256_verified':hashlib.sha256(c.get(a.key)).hexdigest()==digest}
    finally:c.delete(a.key)
result['elapsed_ms']=(time.perf_counter()-t)*1000
print(json.dumps(result))
