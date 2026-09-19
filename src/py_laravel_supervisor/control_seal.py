"""AES-256-GCM using Windows CNG, so Process adapter spools contain ciphertext only.

Keys are per-helper random 32-byte process memory, supplied on stdin. They are
never application credentials, files, environment entries or child MCP inputs.
Reference: Microsoft BCryptEncrypt/BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO.
"""
from __future__ import annotations
import base64
import ctypes
import os
from ctypes import wintypes
from .windows import WindowsProcessError


class AuthInfo(ctypes.Structure):
    _fields_ = [('cbSize',wintypes.ULONG),('dwInfoVersion',wintypes.ULONG),
        ('pbNonce',ctypes.c_void_p),('cbNonce',wintypes.ULONG),
        ('pbAuthData',ctypes.c_void_p),('cbAuthData',wintypes.ULONG),
        ('pbTag',ctypes.c_void_p),('cbTag',wintypes.ULONG),
        ('pbMacContext',ctypes.c_void_p),('cbMacContext',wintypes.ULONG),
        ('cbAAD',wintypes.ULONG),('cbData',ctypes.c_ulonglong),('dwFlags',wintypes.ULONG)]


def seal(data: bytes, key: bytes, aad: bytes) -> dict:
    if os.name != 'nt' or not isinstance(data, bytes) or not 1 <= len(data) <= 1048576 or len(key) != 32 or not 1 <= len(aad) <= 512:
        raise WindowsProcessError('invalid private response envelope')
    bcrypt = ctypes.WinDLL('bcrypt')
    bcrypt.BCryptOpenAlgorithmProvider.argtypes = [ctypes.POINTER(ctypes.c_void_p),wintypes.LPCWSTR,wintypes.LPCWSTR,wintypes.ULONG]
    bcrypt.BCryptSetProperty.argtypes = [ctypes.c_void_p,wintypes.LPCWSTR,ctypes.c_void_p,wintypes.ULONG,wintypes.ULONG]
    bcrypt.BCryptGenerateSymmetricKey.argtypes = [ctypes.c_void_p,ctypes.POINTER(ctypes.c_void_p),ctypes.c_void_p,wintypes.ULONG,ctypes.c_void_p,wintypes.ULONG,wintypes.ULONG]
    bcrypt.BCryptEncrypt.argtypes = [ctypes.c_void_p,ctypes.c_void_p,wintypes.ULONG,ctypes.c_void_p,ctypes.c_void_p,wintypes.ULONG,ctypes.c_void_p,wintypes.ULONG,ctypes.POINTER(wintypes.ULONG),wintypes.ULONG]
    bcrypt.BCryptDestroyKey.argtypes = [ctypes.c_void_p]
    bcrypt.BCryptCloseAlgorithmProvider.argtypes = [ctypes.c_void_p,wintypes.ULONG]
    for name in ['BCryptOpenAlgorithmProvider','BCryptSetProperty','BCryptGenerateSymmetricKey','BCryptEncrypt','BCryptDestroyKey','BCryptCloseAlgorithmProvider']:
        getattr(bcrypt,name).restype = ctypes.c_long
    def check(status):
        if status != 0: raise WindowsProcessError('private response encryption failed')
    algorithm = ctypes.c_void_p(); handle = ctypes.c_void_p()
    secret = ctypes.create_string_buffer(key)
    try:
        check(bcrypt.BCryptOpenAlgorithmProvider(ctypes.byref(algorithm),'AES','Microsoft Primitive Provider',0))
        mode = ctypes.create_unicode_buffer('ChainingModeGCM')
        check(bcrypt.BCryptSetProperty(algorithm,'ChainingMode',mode,ctypes.sizeof(mode),0))
        check(bcrypt.BCryptGenerateSymmetricKey(algorithm,ctypes.byref(handle),None,0,secret,32,0))
        nonce = os.urandom(12); nonce_buffer = ctypes.create_string_buffer(nonce); auth_buffer = ctypes.create_string_buffer(aad)
        tag = ctypes.create_string_buffer(16); source = ctypes.create_string_buffer(data); output = ctypes.create_string_buffer(len(data))
        info = AuthInfo(); info.cbSize = ctypes.sizeof(AuthInfo); info.dwInfoVersion = 1
        info.pbNonce = ctypes.addressof(nonce_buffer); info.cbNonce = 12
        info.pbAuthData = ctypes.addressof(auth_buffer); info.cbAuthData = len(aad)
        info.pbTag = ctypes.addressof(tag); info.cbTag = 16
        size = wintypes.ULONG()
        check(bcrypt.BCryptEncrypt(handle,source,len(data),ctypes.byref(info),None,0,output,len(data),ctypes.byref(size),0))
        if size.value != len(data): raise WindowsProcessError('invalid encrypted response length')
        return {'sealed':1,'nonce':base64.b64encode(nonce).decode(), 'tag':base64.b64encode(tag.raw).decode(),
            'ciphertext':base64.b64encode(output.raw).decode()}
    finally:
        ctypes.memset(secret,0,ctypes.sizeof(secret))
        if handle: bcrypt.BCryptDestroyKey(handle)
        if algorithm: bcrypt.BCryptCloseAlgorithmProvider(algorithm,0)
