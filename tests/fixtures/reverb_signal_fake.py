from __future__ import annotations

import argparse
import os
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["success", "hang"], required=True)
    parser.add_argument("--marker")
    args = parser.parse_args()

    if args.marker:
        Path(args.marker).write_text(
            f"APP_ENV={os.getenv('APP_ENV')}\nOPENAI_API_KEY={os.getenv('OPENAI_API_KEY')}\n",
            encoding="utf-8",
        )

    if args.mode == "hang":
        time.sleep(30)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())