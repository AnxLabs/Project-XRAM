"""Bounded FIFO admission for fixed searches; defaults retain reject-on-busy."""
from collections import deque
import secrets
import threading
import time

class Ticket:
    def __init__(self, request_id):
        self.id=request_id
        self.canceled=threading.Event()

class Scheduler:
    def __init__(self, slots=1, queue_limit=0, wait_ms=1000):
        if type(slots) is not int or not 1<=slots<=2: raise ValueError('invalid search workers')
        if type(queue_limit) is not int or not 0<=queue_limit<=8: raise ValueError('invalid search queue limit')
        if type(wait_ms) is not int or not 1<=wait_ms<=5000: raise ValueError('invalid search queue timeout')
        self.slots=threading.BoundedSemaphore(slots); self.limit=queue_limit; self.wait_ms=wait_ms
        self.condition=threading.Condition();self.waiting=deque();self.tickets={}
    def acquire(self, request_id=None, deadline=None):
        ticket=Ticket(request_id or secrets.token_hex(16));start=time.monotonic()
        end=start+self.wait_ms/1000
        if deadline is not None:end=min(end,deadline)
        with self.condition:
            if ticket.id in self.tickets:return None,'duplicate search id',0.0
            if not self.waiting and self.slots.acquire(blocking=False):
                self.tickets[ticket.id]=ticket
                return ticket,None,(time.monotonic()-start)*1000
            if not self.limit:return None,'search busy',0.0
            if len(self.waiting)>=self.limit:return None,'search queue full',0.0
            self.tickets[ticket.id]=ticket;self.waiting.append(ticket)
            try:
                while True:
                    if ticket.canceled.is_set():return None,'search canceled',(time.monotonic()-start)*1000
                    remaining=end-time.monotonic()
                    if remaining<=0:return None,'search timeout',(time.monotonic()-start)*1000
                    if self.waiting[0] is ticket and self.slots.acquire(blocking=False):
                        self.waiting.popleft();self.condition.notify_all()
                        return ticket,None,(time.monotonic()-start)*1000
                    self.condition.wait(min(remaining,0.05))
            finally:
                if ticket in self.waiting:
                    self.waiting.remove(ticket);self.tickets.pop(ticket.id,None);self.condition.notify_all()
    def release(self,ticket):
        with self.condition:
            self.tickets.pop(ticket.id,None);self.slots.release();self.condition.notify_all()
    def snapshot(self):
        with self.condition:
            return {'search_waiting':len(self.waiting), 'search_active':len(self.tickets)-len(self.waiting)}
    def cancel(self,request_id):
        with self.condition:
            ticket=self.tickets.get(request_id)
            if ticket is None:return False
            ticket.canceled.set();self.condition.notify_all();return True
