import hashlib
import unittest
import zlib
import xram_codec

class CodecTests(unittest.TestCase):
    def test_round_trip(self):
        data = b'\0' * 10000 + b'public fixture'
        digest = hashlib.sha256(data).hexdigest()
        self.assertEqual(xram_codec.decompress(xram_codec.compress(data), size=len(data), sha256=digest), data)

    def test_wrong_digest_or_trailing_data(self):
        data = b'example'; encoded = xram_codec.compress(data)
        with self.assertRaises(ValueError): xram_codec.decompress(encoded, size=len(data), sha256='0'*64)
        with self.assertRaises(ValueError): xram_codec.decompress(encoded+b'extra', size=len(data), sha256=hashlib.sha256(data).hexdigest())

    def test_decompression_limit_and_truncation(self):
        encoded = zlib.compress(b'x'*10000)
        with self.assertRaises(ValueError): xram_codec.decompress(encoded, size=20, sha256='0'*64)
        with self.assertRaises(ValueError): xram_codec.decompress(encoded[:-1], size=10000, sha256=hashlib.sha256(b'x'*10000).hexdigest())
        with self.assertRaises(ValueError): xram_codec.decompress(encoded, size=-1, sha256='0'*64)

if __name__ == '__main__': unittest.main()
