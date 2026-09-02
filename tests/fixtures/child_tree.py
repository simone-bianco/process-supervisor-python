from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=float, default=30.0)
    parser.add_argument("--spawn-child", action="store_true")
    parser.add_argument("--pid-file")
    parser.add_argument("--noise", action="store_true")
    args = parser.parse_args()

    if args.spawn_child:
        child = subprocess.Popen(
            [sys.executable, __file__, "--sleep", str(args.sleep)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if args.pid_file:
            Path(args.pid_file).write_text(f"{os.getpid()}\n{child.pid}\n", encoding="ascii")
    elif args.pid_file:
        Path(args.pid_file).write_text(f"{os.getpid()}\n", encoding="ascii")

    if args.noise:
        sys.stdout.write("Authorization: Bearer split-")
        sys.stdout.flush()
        time.sleep(0.05)
        sys.stdout.write("secret\nnormal stdout\n")
        sys.stdout.flush()
        sys.stderr.write("password=hunter2\nnormal stderr\n")
        sys.stderr.flush()

    time.sleep(args.sleep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())