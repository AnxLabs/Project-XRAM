"""Optional application-side compression for trusted, versioned cache artifacts.

Store the expected uncompressed length and SHA-256 in a trusted local manifest.
The XRAM protocol remains unchanged and verifies the compressed bytes separately.
"""
import hashlib
import hmac
import zlib
from xram import MAX_BYTES

def compress(data):
    if not isinstance(data, (bytes, bytearray, memoryview)) or len(data) > MAX_BYTES:
        raise ValueError('invalid artifact size or type')
    return zlib.compress(data, level=1)

def decompress(data, *, size, sha256):
    if type(size) is not int or not 0 <= size <= MAX_BYTES:
        raise ValueError('invalid expected size')
    if not isinstance(sha256, str) or len(sha256) != 64 or any(c not in '0123456789abcdef' for c in sha256):
        raise ValueError('invalid expected SHA-256')
    if not isinstance(data, (bytes, bytearray, memoryview)) or len(data) > MAX_BYTES:
        raise ValueError('invalid compressed data')
    try:
        decoder = zlib.decompressobj()
        result = decoder.decompress(data, size + 1)
    except zlib.error as exc:
        raise ValueError('invalid compressed artifact') from exc
    if len(result) != size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('compressed artifact length or stream mismatch')
    if not hmac.compare_digest(hashlib.sha256(result).hexdigest(), sha256):
        raise ValueError('uncompressed artifact integrity failure')
    return result
