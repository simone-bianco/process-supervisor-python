"""Explicit Windows isolation primitives for owned local child processes.

These primitives do not register a daemon, select an application store, grant MCP
permissions, or launch registry-supplied commands. The caller must own admission.
"""
from __future__ import annotations

import ctypes
import os
import re
import subprocess
from pathlib import Path

from .windows import WindowsProcessError


class AppContainerProfile:
    def __init__(self, name: str) -> None:
        if os.name != "nt" or re.fullmatch(r"localgpt\.[a-f0-9]{32}", name) is None:
            raise WindowsProcessError("a unique server-owned Windows sandbox identity is required")
        from ctypes import wintypes

        self.name = name
        self._sid = ctypes.c_void_p()
        self._userenv = ctypes.WinDLL("userenv", use_last_error=True)
        self._advapi = ctypes.WinDLL("advapi32", use_last_error=True)
        self._kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self._userenv.CreateAppContainerProfile.argtypes = [
            wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.LPCWSTR,
            ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p),
        ]
        self._userenv.CreateAppContainerProfile.restype = ctypes.c_long
        self._userenv.DeleteAppContainerProfile.argtypes = [wintypes.LPCWSTR]
        self._userenv.DeleteAppContainerProfile.restype = ctypes.c_long
        self._advapi.FreeSid.argtypes = [ctypes.c_void_p]
        self._advapi.FreeSid.restype = ctypes.c_void_p
        self._advapi.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        self._advapi.ConvertSidToStringSidW.restype = wintypes.BOOL
        self._kernel.LocalFree.argtypes = [ctypes.c_void_p]
        self._kernel.LocalFree.restype = ctypes.c_void_p
        result = self._userenv.CreateAppContainerProfile(name, name, "Owned isolated MCP child", None, 0, ctypes.byref(self._sid))
        if result != 0:
            raise WindowsProcessError(f"sandbox profile creation failed (HRESULT {result & 0xffffffff:08x})")
        self._created = True

    @property
    def sid(self) -> int:
        if not self._sid.value:
            raise WindowsProcessError("sandbox profile is closed")
        return self._sid.value

    @property
    def sid_string(self) -> str:
        value = ctypes.c_void_p()
        if not self._advapi.ConvertSidToStringSidW(self.sid, ctypes.byref(value)):
            raise WindowsProcessError("sandbox SID encoding failed")
        try:
            return ctypes.wstring_at(value)
        finally:
            self._kernel.LocalFree(value)

    def grant_directory(self, directory: Path, *, writable: bool = False) -> None:
        """Grant exactly this newly owned directory, never follow a junction/link.

        Admission/root ownership is the caller's responsibility; this is not a
        public arbitrary-filesystem operation. No parent ACL is widened here.
        """
        directory = Path(directory)
        if not directory.is_absolute() or not directory.is_dir():
            raise WindowsProcessError("sandbox directory must already exist")
        for part in (directory, *directory.parents):
            if part.lstat().st_file_attributes & 0x400:
                raise WindowsProcessError("sandbox paths cannot traverse reparse points")
        executable = str(Path(os.environ["SystemRoot"]) / "System32" / "icacls.exe")
        self._acl(executable, [str(directory), "/grant", f"*{self.sid_string}:(OI)(CI)({'M' if writable else 'RX'})"])
        if writable:
            self._acl(executable, [str(directory), "/setintegritylevel", "(OI)(CI)L"])

    @staticmethod
    def _acl(executable: str, args: list[str]) -> None:
        result = subprocess.run(
            [executable, *args], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=10, check=False, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if result.returncode != 0:
            raise WindowsProcessError("sandbox filesystem ACL configuration failed")

    def close(self) -> None:
        """Delete only this created OS profile after the caller proves child quiescence."""
        if self._created:
            result = self._userenv.DeleteAppContainerProfile(self.name)
            if result != 0:
                raise WindowsProcessError(f"sandbox profile cleanup failed (HRESULT {result & 0xffffffff:08x})")
            self._created = False
        if self._sid.value:
            self._advapi.FreeSid(self._sid)
            self._sid = ctypes.c_void_p()
