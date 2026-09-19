"""Local Windows byte IPC with explicit owner ACL and cancellable overlapped I/O.

This transports closed control frames, never unpickles objects or interprets MCP.
No daemon, TCP listener, credentials database, or public endpoint is introduced.
"""
from __future__ import annotations

import ctypes
import os
import re
import struct
import time
from ctypes import wintypes
from .windows import WindowsProcessError


class Overlapped(ctypes.Structure):
    _fields_ = [('Internal', ctypes.c_size_t), ('InternalHigh', ctypes.c_size_t),
                ('Offset', wintypes.DWORD), ('OffsetHigh', wintypes.DWORD), ('hEvent', wintypes.HANDLE)]


class SecurityAttributes(ctypes.Structure):
    _fields_ = [('nLength', wintypes.DWORD), ('lpSecurityDescriptor', ctypes.c_void_p), ('bInheritHandle', wintypes.BOOL)]


class PrivatePipe:
    MAX_PACKET = 2 * 1024 * 1024

    def __init__(self, name: str, *, server: bool, server_pid: int | None = None):
        if os.name != 'nt' or re.fullmatch(r'\\\\\.\\pipe\\LocalGptMcp-[a-f0-9]{64}', name) is None:
            raise WindowsProcessError('invalid owned local pipe')
        self.name = name
        self.server = server
        self.handle = None
        self._retained_io = []
        k = self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        a = self.advapi = ctypes.WinDLL('advapi32', use_last_error=True)
        k.CreateEventW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR]; k.CreateEventW.restype = wintypes.HANDLE
        k.CloseHandle.argtypes = [wintypes.HANDLE]; k.CloseHandle.restype = wintypes.BOOL
        k.CreateNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(SecurityAttributes)]
        k.CreateNamedPipeW.restype = wintypes.HANDLE
        k.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]; k.CreateFileW.restype = wintypes.HANDLE
        k.ConnectNamedPipe.argtypes = [wintypes.HANDLE, ctypes.POINTER(Overlapped)]; k.ConnectNamedPipe.restype = wintypes.BOOL
        k.DisconnectNamedPipe.argtypes = [wintypes.HANDLE]; k.DisconnectNamedPipe.restype = wintypes.BOOL
        for method in ('ReadFile', 'WriteFile'):
            fn = getattr(k, method); fn.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(Overlapped)]; fn.restype = wintypes.BOOL
        k.CancelIoEx.argtypes = [wintypes.HANDLE, ctypes.POINTER(Overlapped)]; k.CancelIoEx.restype = wintypes.BOOL
        k.GetOverlappedResult.argtypes = [wintypes.HANDLE, ctypes.POINTER(Overlapped), ctypes.POINTER(wintypes.DWORD), wintypes.BOOL]; k.GetOverlappedResult.restype = wintypes.BOOL
        k.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]; k.WaitForSingleObject.restype = wintypes.DWORD
        k.GetNamedPipeServerProcessId.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.ULONG)]; k.GetNamedPipeServerProcessId.restype = wintypes.BOOL
        k.WaitNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD]; k.WaitNamedPipeW.restype = wintypes.BOOL
        k.GetCurrentProcess.restype = wintypes.HANDLE
        k.LocalFree.argtypes = [ctypes.c_void_p]; k.LocalFree.restype = ctypes.c_void_p
        if server:
            descriptor = self._owner_descriptor()
            attributes = SecurityAttributes(ctypes.sizeof(SecurityAttributes), descriptor, False)
            try:
                # FIRST_PIPE_INSTANCE prevents adoption; reject remote clients and use no inheritable handle.
                self.handle = k.CreateNamedPipeW(name, 0x00000003 | 0x40000000 | 0x00080000,
                    0x00000008, 1, 65536, 65536, 0, ctypes.byref(attributes))
            finally:
                k.LocalFree(descriptor)
        else:
            if not isinstance(server_pid, int) or server_pid < 1:
                raise WindowsProcessError('original server process required')
            if not k.WaitNamedPipeW(name, 2000):
                raise WindowsProcessError('original local pipe unavailable')
            self.handle = k.CreateFileW(name, 0xc0000000, 0, None, 3, 0x40000000, None)
        if not self.handle or self.handle == ctypes.c_void_p(-1).value:
            self.handle = None
            raise WindowsProcessError('owned local pipe creation failed')
        if not server:
            actual = wintypes.ULONG()
            if not k.GetNamedPipeServerProcessId(self.handle, ctypes.byref(actual)) or actual.value != server_pid:
                self.close()
                raise WindowsProcessError('local pipe process identity changed')

    def _owner_descriptor(self):
        a = self.advapi; k = self.kernel
        a.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]; a.OpenProcessToken.restype = wintypes.BOOL
        a.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]; a.GetTokenInformation.restype = wintypes.BOOL
        a.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]; a.ConvertSidToStringSidW.restype = wintypes.BOOL
        a.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p]; a.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = wintypes.BOOL
        token = wintypes.HANDLE(); size = wintypes.DWORD()
        if not a.OpenProcessToken(k.GetCurrentProcess(), 8, ctypes.byref(token)):
            raise WindowsProcessError('cannot identify IPC owner')
        try:
            a.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            if not 1 <= size.value <= 4096:
                raise WindowsProcessError('invalid IPC owner token')
            buffer = ctypes.create_string_buffer(size.value)
            if not a.GetTokenInformation(token, 1, buffer, size.value, ctypes.byref(size)):
                raise WindowsProcessError('cannot identify IPC owner')
            sid = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p))[0]
            string = ctypes.c_void_p()
            if not a.ConvertSidToStringSidW(sid, ctypes.byref(string)):
                raise WindowsProcessError('cannot encode IPC owner')
            try: user = ctypes.wstring_at(string)
            finally: k.LocalFree(string)
            descriptor = ctypes.c_void_p()
            if not a.ConvertStringSecurityDescriptorToSecurityDescriptorW('D:P(A;;GA;;;SY)(A;;GA;;;' + user + ')', 1, ctypes.byref(descriptor), None):
                raise WindowsProcessError('cannot constrain IPC ownership')
            return descriptor
        finally:
            k.CloseHandle(token)

    def _operation(self, method: str, buffer, size: int, deadline: float) -> int:
        if self.handle is None or time.monotonic() >= deadline:
            raise WindowsProcessError('owned IPC closed or expired')
        event = self.kernel.CreateEventW(None, True, False, None)
        if not event: raise WindowsProcessError('owned IPC event failed')
        operation = Overlapped(); operation.hEvent = event; transferred = wintypes.DWORD()
        try:
            if method == 'ConnectNamedPipe':
                ok = self.kernel.ConnectNamedPipe(self.handle, ctypes.byref(operation))
            else:
                ok = getattr(self.kernel, method)(self.handle, buffer, size, ctypes.byref(transferred), ctypes.byref(operation))
            error = ctypes.get_last_error() if not ok else 0
            if method == 'ConnectNamedPipe' and error == 535: return 0
            if not ok and error != 997: raise WindowsProcessError('owned IPC interrupted')
            if not ok:
                remaining = max(1, min(60000, int((deadline - time.monotonic()) * 1000)))
                if self.kernel.WaitForSingleObject(event, remaining) != 0:
                    self.kernel.CancelIoEx(self.handle, ctypes.byref(operation))
                    if self.kernel.WaitForSingleObject(event, 1000) != 0:
                        self._retained_io.append((buffer, operation, event))
                        event = None
                    raise WindowsProcessError('owned IPC deadline exceeded')
            if not self.kernel.GetOverlappedResult(self.handle, ctypes.byref(operation), ctypes.byref(transferred), False):
                raise WindowsProcessError('owned IPC result unavailable')
            return transferred.value
        finally:
            if event: self.kernel.CloseHandle(event)

    def accept(self, deadline: float) -> None:
        if not self.server: raise WindowsProcessError('not an IPC server')
        self._operation('ConnectNamedPipe', None, 0, deadline)

    def disconnect(self) -> None:
        if self.handle is not None and self.server:
            self.kernel.DisconnectNamedPipe(self.handle)

    def _read(self, count: int, deadline: float) -> bytes:
        result = bytearray()
        while len(result) < count:
            buffer = ctypes.create_string_buffer(min(65536, count - len(result)))
            read = self._operation('ReadFile', buffer, len(buffer), deadline)
            if read < 1: raise WindowsProcessError('owned IPC unexpected EOF')
            result.extend(buffer.raw[:read])
        return bytes(result)

    def receive(self, deadline: float) -> bytes:
        size = struct.unpack('>I', self._read(4, deadline))[0]
        if not 1 <= size <= self.MAX_PACKET: raise WindowsProcessError('owned IPC frame budget exceeded')
        return self._read(size, deadline)

    def send(self, data: bytes, deadline: float) -> None:
        if not isinstance(data, bytes) or not 1 <= len(data) <= self.MAX_PACKET:
            raise WindowsProcessError('owned IPC frame budget exceeded')
        packet = struct.pack('>I', len(data)) + data; offset = 0
        while offset < len(packet):
            chunk = packet[offset:offset + 65536]; buffer = ctypes.create_string_buffer(chunk)
            written = self._operation('WriteFile', buffer, len(chunk), deadline)
            if written < 1: raise WindowsProcessError('owned IPC short write')
            offset += written

    def close(self) -> None:
        if self.handle is not None:
            self.kernel.CancelIoEx(self.handle, None)
            self.kernel.CloseHandle(self.handle)
            self.handle = None
        # Uncertain buffers stay referenced until this private process exits, never reused.
