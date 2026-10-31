#!/usr/bin/env python3
#  Project:      dfe-loader
#  File:         scripts/capture-clickhouse-fixtures.py
#  Purpose:      Re-capture the ClickHouse error bodies the classifier tests
#                read, from the run's shared ClickHouse container.
#  Language:     Python
#
#  License:      BUSL-1.1
#  Copyright:    (c) 2026 HYPERI PTY LIMITED
"""Re-capture tests/fixtures/clickhouse/errors/ from a live ClickHouse.

    scripts/capture-clickhouse-fixtures.py            # capture, then show the diff
    scripts/capture-clickhouse-fixtures.py --check    # compare only, write nothing

It runs one full-tier test, integration::clickhouse_error_bodies, which starts
the suite's shared ClickHouse (the image pinned in tests/common/containers.rs),
makes the server raise each error the classifier table uses, and writes each
body over its fixture. The 319 fixture is not captured: it needs a lost Keeper
session, and stays as written.

Run it where the fixtures should land: the test writes into this checkout, so
a cargo that builds on another host (cargo-remote) writes there instead. The
container runs on whatever Docker the environment names (DOCKER_HOST or the
current docker context).
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FIXTURES = REPO / "tests" / "fixtures" / "clickhouse" / "errors"
TEST = "integration::clickhouse_error_bodies::the_server_still_answers_as_the_fixtures_say"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="compare the live codes and names with the fixtures, write nothing",
    )
    parser.add_argument(
        "--cargo",
        default="cargo",
        help="cargo command to run the test with (default: cargo)",
    )
    args = parser.parse_args()

    env = dict(os.environ)
    if args.check:
        env.pop("DFE_LOADER_CAPTURE_FIXTURES", None)
    else:
        env["DFE_LOADER_CAPTURE_FIXTURES"] = "1"
    cmd = [
        args.cargo,
        "nextest",
        "run",
        "--all-features",
        "--run-ignored",
        "all",
        "--ignore-default-filter",
        "--no-capture",
        "-E",
        f"test(={TEST})",
    ]
    print("running:", " ".join(cmd), file=sys.stderr)
    rc = subprocess.run(cmd, cwd=REPO, env=env, check=False).returncode
    if rc != 0:
        print(f"the capture test failed with exit {rc}", file=sys.stderr)
        return rc
    if not args.check:
        diff = subprocess.run(
            ["git", "diff", "--stat", "--", str(FIXTURES)],
            cwd=REPO,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        print(diff.stdout or "fixtures unchanged")
        print("Review the diff, then run the classifier table: cargo nextest run --lib error")
    return 0


if __name__ == "__main__":
    sys.exit(main())
