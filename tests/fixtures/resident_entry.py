from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from py_laravel_supervisor.resident import SupervisorResident


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--installation-id", required=True)
    parser.add_argument("--incarnation", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--ready-nonce", required=True)
    parser.add_argument("--anchor-job-name", required=True)
    parser.add_argument("--startup-delay", type=float, default=0.0)
    args = parser.parse_args()
    if args.startup_delay > 0:
        time.sleep(min(args.startup_delay, 2.0))
    return SupervisorResident(
        runtime_root=args.runtime_root,
        installation_id=args.installation_id,
        incarnation=args.incarnation,
        attempt_id=args.attempt_id,
        ready_nonce=args.ready_nonce,
        anchor_name=args.anchor_job_name,
        poll_interval_seconds=0.05,
    ).run()


if __name__ == "__main__":
    raise SystemExit(main())