import threading,time,unittest
from search_scheduler import Scheduler

def until(predicate):
    end=time.monotonic()+2
    while time.monotonic()<end:
        if predicate():return
        time.sleep(.002)
    raise AssertionError('condition not reached')

class SchedulerTests(unittest.TestCase):
    def test_queue_full_timeout_and_release(self):
        q=Scheduler(1,1,60);first,error,_=q.acquire('a'*32);self.assertIsNone(error);out=[]
        t=threading.Thread(target=lambda:out.append(q.acquire('b'*32)));t.start();until(lambda:len(q.waiting)==1)
        self.assertEqual(q.acquire('c'*32)[1],'search queue full');t.join(1)
        self.assertEqual(out[0][1],'search timeout');q.release(first);self.assertFalse(q.tickets)
    def test_cancel_queued_and_duplicate(self):
        q=Scheduler(1,1,1000);first,_,_=q.acquire('a'*32);out=[]
        t=threading.Thread(target=lambda:out.append(q.acquire('b'*32)));t.start();until(lambda:len(q.waiting)==1)
        self.assertEqual(q.acquire('b'*32)[1],'duplicate search id');self.assertTrue(q.cancel('b'*32));t.join(1)
        self.assertEqual(out[0][1],'search canceled');self.assertFalse(q.cancel('b'*32));q.release(first);self.assertFalse(q.tickets)
    def test_fifo_and_two_slots(self):
        q=Scheduler(1,2,1000);first,_,_=q.acquire('a'*32);out=[]
        def run(name):
            ticket,error,_=q.acquire(name*32);out.append((name,error));
            if ticket:q.release(ticket)
        b=threading.Thread(target=run,args=('b',));b.start();until(lambda:len(q.waiting)==1)
        c=threading.Thread(target=run,args=('c',));c.start();until(lambda:len(q.waiting)==2)
        q.release(first);b.join(2);c.join(2);self.assertEqual(out,[('b',None),('c',None)])
        p=Scheduler(2,0,1);a,_,_=p.acquire();b,_,_=p.acquire();self.assertEqual(p.acquire()[1],'search busy');p.release(a);p.release(b)
    def test_invalid_bounds(self):
        for args in [(3,0,1),(1,9,1),(1,1,5001),(True,0,1)]:
            with self.assertRaises(ValueError):Scheduler(*args)

if __name__=='__main__':unittest.main()
