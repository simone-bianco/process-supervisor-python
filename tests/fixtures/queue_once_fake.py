from __future__ import annotations

import argparse
import json
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["empty", "success", "failed", "released", "crash", "hang"], required=True)
    parser.add_argument("--sleep", type=float, default=0.05)
    args = parser.parse_args()
    if args.mode == "empty":
        time.sleep(args.sleep)
        return 0
    if args.mode == "crash":
        return 7
    starting = {
        "job": "App\\Jobs\\Fixture",
        "queue": "default",
        "connection": "database",
        "attempts": 1,
        "status": "starting",
        "message": "password=fixture-secret",
    }
    print(json.dumps(starting), flush=True)
    if args.mode == "hang":
        time.sleep(30)
        return 0
    time.sleep(args.sleep)
    terminal_status = {
        "success": "success",
        "failed": "failed",
        "released": "released_after_exception",
    }[args.mode]
    payload = {
        **starting,
        "status": terminal_status,
        "result": {"success": "deleted", "failed": "failed", "released": "released"}[args.mode],
    }
    print(json.dumps(payload), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())