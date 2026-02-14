#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ts2date.py - interactive timestamp -> datetime converter

Usage:
  python3 ts2date.py            # interactive mode
  python3 ts2date.py 1700000000 # one-shot (optional)

Input formats supported:
  - seconds / milliseconds / microseconds / nanoseconds (auto-detected by digit length)
  - optional timezone:
      ts 1700000000
      ts 1700000000 UTC
      ts 1700000000 +08:00
      ts 1700000000000 Asia/Singapore  (if zoneinfo available)

Commands:
  - q / quit / exit : leave
  - help            : show help
"""

from __future__ import annotations

import sys
import re
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


_NUM_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")


def _parse_tz(tz_token: str | None):
    """Return a tzinfo. Accepts: None, 'UTC', '+08:00', '-0530', 'Asia/Singapore'."""
    if not tz_token:
        return datetime.now().astimezone().tzinfo

    t = tz_token.strip()
    if not t:
        return datetime.now().astimezone().tzinfo

    if t.upper() in ("UTC", "Z"):
        return timezone.utc

    # Offset like +08:00 or -0530
    m = re.match(r"^([+-])(\d{2}):?(\d{2})$", t)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        hh = int(m.group(2))
        mm = int(m.group(3))
        return timezone(sign * timedelta(hours=hh, minutes=mm))

    # IANA zone like Asia/Singapore
    if ZoneInfo is not None:
        try:
            return ZoneInfo(t)
        except Exception:
            pass

    raise ValueError(f"Unrecognized timezone: {tz_token!r}. Try UTC, +08:00, or an IANA zone like Asia/Singapore.")


def _infer_unit_and_seconds(ts_str: str) -> tuple[str, float]:
    """
    Infer unit by digit length of integer part:
      10 digits  -> seconds
      13 digits  -> milliseconds
      16 digits  -> microseconds
      19 digits  -> nanoseconds
    Also supports decimals (treated as seconds).
    """
    s = ts_str.strip()
    if not _NUM_RE.match(s):
        raise ValueError(f"Not a number: {ts_str!r}")

    # If has decimal point, treat as seconds float
    if "." in s:
        return "seconds(float)", float(s)

    neg = s.startswith("-")
    digits = s[1:] if neg else s

    # Heuristic by length
    n = len(digits)
    v = int(s)

    if n <= 10:
        return "seconds", float(v)
    if 11 <= n <= 13:
        return "milliseconds", v / 1_000.0
    if 14 <= n <= 16:
        return "microseconds", v / 1_000_000.0
    if 17 <= n <= 19:
        return "nanoseconds", v / 1_000_000_000.0

    # Fallback: assume seconds
    return f"seconds(heuristic, len={n})", float(v)


def convert(ts_str: str, tz_token: str | None):
    unit, seconds = _infer_unit_and_seconds(ts_str)
    tz = _parse_tz(tz_token)
    dt = datetime.fromtimestamp(seconds, tz=tz)
    dt_utc = datetime.fromtimestamp(seconds, tz=timezone.utc)

    return {
        "input": ts_str,
        "unit": unit,
        "tz": str(tz),
        "local": dt.isoformat(sep=" ", timespec="seconds"),
        "utc": dt_utc.isoformat(sep=" ", timespec="seconds"),
        "epoch_seconds": seconds,
    }


def print_help():
    print(
        "\nExamples:\n"
        "  1700000000\n"
        "  1700000000000\n"
        "  1700000000 UTC\n"
        "  1700000000 +08:00\n"
        "  1700000000 Asia/Singapore\n"
        "\nCommands: help, q, quit, exit\n"
    )


def interactive():
    print("ts2date interactive. Input: <timestamp> [timezone]. Type 'help' for examples, 'q' to quit.")
    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not line:
            continue
        if line.lower() in ("q", "quit", "exit"):
            return
        if line.lower() in ("h", "help", "?"):
            print_help()
            continue

        parts = line.split()
        ts = parts[0]
        tz_token = parts[1] if len(parts) >= 2 else None

        try:
            out = convert(ts, tz_token)
            print(
                # f"unit={out['unit']} tz={out['tz']}\n"
                f"local={out['local']}\n"
                # f"utc  ={out['utc']}\n"
                # f"epoch_seconds={out['epoch_seconds']}\n"
            )
        except Exception as e:
            print(f"error: {e}")


def main(argv: list[str]) -> int:
    if len(argv) >= 2:
        ts = argv[1]
        tz_token = argv[2] if len(argv) >= 3 else None
        try:
            out = convert(ts, tz_token)
            print(
                f"unit={out['unit']} tz={out['tz']}\n"
                f"local={out['local']}\n"
                f"utc  ={out['utc']}\n"
                f"epoch_seconds={out['epoch_seconds']}"
            )
            return 0
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
            return 2

    interactive()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
