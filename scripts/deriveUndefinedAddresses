# TODO: In the future, this function should be able to input and derive the addresses of descriptors as well. This would allow for the codebase to be significantly simpler.
import hashlib
import coincurve
from base58 import b58decode_check
from bip32utils import BIP32Key
# SOURCE: https://developer.bitcoin.org/devguide/transactions.html
# 2.7x faster than bitcoinlib.
# Constants used for Bech32 encoding (fixed by BIP-173). 
_CHARSET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l'
_HRP = 'bc'
_HRP_EXPAND = [ord(x) >> 5 for x in _HRP] + [0] + [ord(x) & 31 for x in _HRP]
_GEN = (0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3)
_B58_ALPHABET = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

# Hashlib cached constructors for speed boost and non redundantness
_sha256 = hashlib.sha256
_ripemd160 = lambda: hashlib.new('ripemd160')


def _hash160(b: bytes) -> bytes:
    """Perform Bitcoin HASH160 (RIPEMD160 of SHA256 of input)."""
    h = _sha256()
    h.update(b)
    s = h.digest()
    r = _ripemd160()
    r.update(s)
    return r.digest()

def _base58checkEncode(data: bytes) -> str:
    """Encode data with Base58Check (double SHA256 + Base58)."""
    checksum = _sha256(_sha256(data).digest()).digest()[:4]
    data += checksum
    n = int.from_bytes(data, 'big')
    res = bytearray()
    while n > 0:
        n, r = divmod(n, 58)
        res.append(_B58_ALPHABET[r])
    res.reverse()
    # Preserve leading 0x00 bytes as '1'
    pad = 0
    for byte in data:
        if byte == 0:
            pad += 1
        else:
            break
    return (b'1' * pad + res).decode()

def _bech32Polymod(values):
    """Calculate Bech32 checksum polymod."""
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1ffffff) << 5 ^ v
        for i in range(5):
            if (b >> i) & 1:
                chk ^= _GEN[i]
    return chk

def _bech32CreateChecksum(data):
    """Create a checksum for Bech32 encoding."""
    values = _HRP_EXPAND + data
    polymod = _bech32Polymod(values + [0, 0, 0, 0, 0, 0]) ^ 1
    return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]

def _bech32Encode(data):
    """Encode payload bytes into Bech32 address string."""
    combined = data + _bech32CreateChecksum(data)
    return _HRP + '1' + ''.join(_CHARSET[d] for d in combined)

def _convertBits(data, fromBits, toBits):
    """Convert bit groups (e.g., 8-bit bytes to 5-bit groups for Bech32)."""
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << toBits) - 1
    for v in data:
        acc = (acc << fromBits) | v
        bits += fromBits
        while bits >= toBits:
            bits -= toBits
            ret.append((acc >> bits) & maxv)
    if bits:
        ret.append((acc << (tobits - bits)) & maxv)
    return ret



def compressPubkey(pubkeyBytes: bytes) -> bytes:
    """Compress an uncompressed public key (0x04 + x + y format)."""
    v = memoryview(pubkeyBytes)
    return (b'\x02' if (v[64] & 1) == 0 else b'\x03') + v[1:33].tobytes()

def decompressPubkey(pubkeyBytes: bytes) -> bytes:
    """Decompress a compressed public key using secp256k1."""
    return coincurve.PublicKey(pubkeyBytes).format(compressed=False)

def p2pkhAddress(pubkeyBytes: bytes) -> str:
    """Create a P2PKH (legacy Base58) address from a public key."""
    return _base58checkEncode(b'\x00' + _hash160(pubkeyBytes))

def p2wpkhAddress(pubkeyBytes: bytes) -> str:
    """Create a P2WPKH (SegWit Bech32) address from a public key."""
    h160 = _hash160(pubkeyBytes)
    data = _convertBits(h160, 8, 5)
    return _bech32Encode([0] + data)

def deriveUndefinedAddresses(pubkey, assumeMultisigOwned=True, nChildKeys=2):
    def _isXpub(k: str):
        return k[:4] in ('xpub', 'tpub', 'ypub', 'zpub', 'vpub')

    def _deriveFromXpub(xpub: str, n: int):
        bip32 = BIP32.from_xpub(xpub)
        results = []
        for i in range(n):
            pubkey = bip32.get_pubkey_from_path(f"m/{i}")
            uncompressed = decompressPubkey(pubkey)
            results.extend([
                uncompressed.hex(),
                pubkey.hex(),
                p2pkhAddress(uncompressed),
                p2pkhAddress(pubkey),
                p2wpkhAddress(pubkey),
            ])
        return results

    def _derive(pubkeyHex: str):
        pubkeyBytes = bytes.fromhex(pubkeyHex)
        if pubkeyBytes[0] == 0x04:
            uncompressed = pubkeyBytes
            compressed = compressPubkey(pubkeyBytes)
        else:
            compressed = pubkeyBytes
            uncompressed = decompressPubkey(pubkeyBytes)

        return [
            uncompressed.hex(),
            compressed.hex(),
            p2pkhAddress(uncompressed),
            p2pkhAddress(compressed),
            p2wpkhAddress(compressed)
        ]

    if isinstance(pubkey, list):
        tree = []
        for k in pubkey:
            if _isXpub(k):
                tree.append(_deriveFromXpub(k, nChildKeys))
            else:
                tree.append(_derive(k))
        return sum(tree, []) if assumeMultisigOwned else tree
    elif _isXpub(pubkey):
        return _deriveFromXpub(k, nChildKeys)
    else:
        return _derive(pubkey)
