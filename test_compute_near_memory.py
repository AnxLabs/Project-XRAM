import unittest
from unittest.mock import patch
import compute_near_memory as near

class SearchClientTests(unittest.TestCase):
    def test_rejects_wrong_artifact_and_malformed_result(self):
        session=near.Session(None); digest='a'*64
        with patch.object(session,'request',return_value={'sha256':'b'*64,'results':[[1.0,0]]}):
            with self.assertRaises(ValueError): session.search('key',0,1,digest)
        for rows in ([], [[float('nan'),0]], [[1.0,-1]], [[1.0,0],[0.1,1]]):
            with patch.object(session,'request',return_value={'sha256':digest,'results':rows}):
                with self.assertRaises(ValueError): session.search('key',0,1,digest)
    def test_cancellation_client_validates_id_and_response(self):
        session=near.Session(None)
        with patch.object(session,'request',return_value={'canceled':True}) as request:
            self.assertTrue(session.cancel('c'*32))
            request.assert_called_once_with('cancel_search',None,request_id='c'*32)
        with self.assertRaises(ValueError): session.cancel('bad')
        with patch.object(session,'request',return_value={'canceled':'yes'}):
            with self.assertRaises(ValueError): session.cancel('c'*32)

    def test_rejects_invalid_query_without_sending(self):
        session=near.Session(None)
        with patch.object(session,'request') as request:
            with self.assertRaises(ValueError): session.search('key',True,1,'a'*64)
            with self.assertRaises(ValueError): session.search('key',0,11,'a'*64)
            request.assert_not_called()

if __name__=='__main__': unittest.main()
