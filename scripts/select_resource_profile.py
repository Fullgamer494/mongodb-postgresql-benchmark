#!/usr/bin/env python3
"""Render Compose resource limits from the canonical profile JSON."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILES = ROOT / "config/resource_profiles.json"
DEFAULT_OUTPUT = ROOT / ".env.resources"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--profiles", type=Path, default=DEFAULT_PROFILES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    profiles = json.loads(args.profiles.read_text(encoding="utf-8"))
    if args.profile not in profiles:
        parser.error(f"perfil desconocido: {args.profile}")
    profile = profiles[args.profile]
    cpus = float(profile["cpus"])
    memory_mib = int(profile["memory_mib"])
    if cpus <= 0 or memory_mib <= 0:
        parser.error("los límites del perfil deben ser positivos")
    payload = (
        f"LAB_PROFILE={args.profile}\n"
        f"LAB_CPUS={cpus:g}\n"
        f"LAB_MEMORY_MIB={memory_mib}\n"
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{args.output.name}.", dir=args.output.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, args.output)
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    print(f"Perfil {args.profile}: {cpus:g} CPU, {memory_mib} MiB RAM/swap total → {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
