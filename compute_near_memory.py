"""Client for the optional, fixed XRAM vector-search operation."""
import hashlib
import hmac
import math
import xram

class Session:
    def __init__(self, client):
        self.client=client; self.conn=None; self.requests=0; self.connections=0
        self.uploaded=0; self.downloaded=0
    def close(self):
        if self.conn is not None: self.conn.close(); self.conn=None
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    def request(self, op, key, **fields):
        if self.conn is None or self.requests >= xram.MAX_CONNECTION_REQUESTS:
            self.close(); self.conn=self.client.connect(); self.requests=0; self.connections+=1
        request=dict(op=op, token=self.client.token, keep=True, **fields)
        if key is not None: request['key']=key
        import json
        size=lambda obj: 4+len(json.dumps(obj,separators=(',',':')).encode())
        xram.send_msg(self.conn,request); self.uploaded+=size(request)
        response=xram.recv_msg(self.conn); self.downloaded+=size(response); self.requests+=1
        if not response.get('ok'): raise xram.CacheError(response.get('error','operation failed'))
        if op=='get':
            n=response.get('size')
            if type(n) is not int or not 0<=n<=xram.MAX_BYTES: raise ValueError('invalid data length')
            data=xram.read_exact(self.conn,n); self.downloaded+=n
            if not hmac.compare_digest(hashlib.sha256(data).hexdigest(),response.get('sha256','')): raise ValueError('transport integrity failure')
            return data
        return response
    def search(self,key,index,top,expected_sha256,*,request_id=None):
        if type(index) is not int or index<0 or type(top) is not int or not 1<=top<=10: raise ValueError('invalid search arguments')
        fields=dict(query_index=index,top=top)
        if request_id is not None:
            if not isinstance(request_id,str) or len(request_id)!=32 or any(c not in '0123456789abcdef' for c in request_id): raise ValueError('invalid search id')
            fields['request_id']=request_id
        response=self.request('search',key,**fields)
        digest=response.get('sha256'); rows=response.get('results')
        if not isinstance(digest,str) or not hmac.compare_digest(digest,expected_sha256): raise ValueError('artifact digest mismatch')
        if not isinstance(rows,list) or len(rows)>top or not rows: raise ValueError('invalid search result')
        for row in rows:
            if not isinstance(row,list) or len(row)!=2 or type(row[0]) not in (float,int) or not math.isfinite(row[0]) or type(row[1]) is not int or row[1]<0: raise ValueError('invalid search result')
        return rows
    def cancel(self, request_id):
        if not isinstance(request_id,str) or len(request_id)!=32 or any(c not in '0123456789abcdef' for c in request_id): raise ValueError('invalid search id')
        response=self.request('cancel_search',None,request_id=request_id)
        if type(response.get('canceled')) is not bool: raise ValueError('invalid cancellation response')
        return response['canceled']
