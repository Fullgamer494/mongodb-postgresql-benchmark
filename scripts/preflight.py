#!/usr/bin/env python3
"""Check Docker host, target container readiness and effective cgroups v2 limits."""

from __future__ import annotations

import argparse
import json
import math
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PROFILES_PATH = ROOT / "config/resource_profiles.json"
PROJECT_LABEL = "u2-benchmark"
PINNED_IMAGES = {
    "postgresql": "postgres:18.6-trixie@sha256:f4b419e350fd98d4a8429603295208976dea4c8fb8326d818d346d3e29f1db6f",
    "mongodb": "mongodb/mongodb-community-server:9.0.4-ubi8-slim@sha256:a25ebe616b146f41dfa3ccd90256e065494414b69cc7641119bea346b0c5d2ff",
}


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def kernel_tuple(release: str) -> tuple[int, int, int]:
    parts = release.split("-", 1)[0].split(".")
    try:
        return tuple(int(part) for part in parts[:3])  # type: ignore[return-value]
    except ValueError:
        return (0, 0, 0)


def version_tuple(value: str) -> tuple[int, int, int] | None:
    match = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?", value)
    if not match:
        return None
    return tuple(int(part or 0) for part in match.groups())  # type: ignore[return-value]


def mongodb_compatibility(kernel_release: str, os_release: dict[str, str]) -> dict[str, Any]:
    """Evaluate the kernel guard, including MongoDB's Ubuntu backport exception."""
    kernel = kernel_tuple(kernel_release)
    image = PINNED_IMAGES["mongodb"]
    image_version = re.search(r":(\d+\.\d+\.\d+)(?:-|@)", image)
    mongo_version = version_tuple(image_version.group(1)) if image_version else None
    distribution = os_release.get("ID", "").lower()
    distro_version_raw = os_release.get("VERSION_ID", "")
    distro_version = version_tuple(distro_version_raw)

    ubuntu_exception = (
        distribution == "ubuntu"
        and distro_version is not None
        and distro_version >= (24, 4, 0)
        and mongo_version is not None
        and mongo_version >= (9, 0, 4)
    )
    affected_kernel = (6, 19, 0) <= kernel < (7, 0, 14)
    compatible = kernel != (0, 0, 0) and (not affected_kernel or ubuntu_exception)
    if kernel == (0, 0, 0):
        reason = "no se pudo interpretar la versión del kernel"
    elif not affected_kernel:
        reason = "kernel fuera del intervalo afectado 6.19–7.0.13"
    elif ubuntu_exception:
        reason = "excepción oficial Ubuntu 24.04+ con MongoDB 9.0.4+ (SERVER-131779/SERVER-125742)"
    else:
        reason = "kernel en intervalo afectado y sin excepción oficial para la distribución/versión fijada"

    return {
        "compatible": compatible,
        "kernel_release": kernel_release,
        "distribution": distribution or None,
        "distribution_version": distro_version_raw or None,
        "mongodb_image": image,
        "mongodb_version": image_version.group(1) if image_version else None,
        "rule": reason,
        "evidence": ["SERVER-131779", "SERVER-125742"],
    }


def read_cgroup_v2(pid: int) -> tuple[Path | None, dict[str, str]]:
    membership = Path(f"/proc/{pid}/cgroup")
    try:
        entries = membership.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None, {}
    relative = next((line.split("::", 1)[1] for line in entries if "::" in line), None)
    if relative is None:
        return None, {}
    group = Path("/sys/fs/cgroup") / relative.lstrip("/")
    values: dict[str, str] = {}
    for filename in ("cpu.max", "memory.max", "memory.swap.max", "memory.events", "cpu.stat", "cpuset.cpus.effective"):
        try:
            values[filename] = (group / filename).read_text(encoding="utf-8").strip()
        except OSError:
            continue
    return group if group.exists() else None, values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    parser.add_argument("--profile", required=True)
    parser.add_argument("--host-only", action="store_true", help="Validate Docker and host compatibility without requiring a running service")
    parser.add_argument("--output", type=Path, help="Write a JSON evidence report; refuses to overwrite")
    args = parser.parse_args()
    profiles = json.loads(PROFILES_PATH.read_text(encoding="utf-8"))
    if args.profile not in profiles:
        parser.error(f"perfil desconocido: {args.profile}")
    expected = profiles[args.profile]
    result: dict[str, Any] = {
        "engine": args.engine,
        "profile": args.profile,
        "kernel": platform.release(),
        "os_release": platform.freedesktop_os_release(),
        "checks": [],
        "ready": False,
    }
    blockers: list[str] = []

    info = run(["docker", "info", "--format", "{{.ServerVersion}}|{{.CgroupVersion}}|{{.CgroupDriver}}|{{.NCPU}}|{{.MemTotal}}"])
    if info.returncode != 0:
        blockers.append(f"Docker daemon inaccesible: {info.stderr.strip()}")
    else:
        pieces = info.stdout.strip().split("|")
        if len(pieces) == 5:
            version, cgroup_version, cgroup_driver, cpus, memory = pieces
            result["docker"] = {
                "server_version": version,
                "cgroup_version": cgroup_version,
                "cgroup_driver": cgroup_driver,
                "cpus": int(cpus),
                "memory_bytes": int(memory),
            }
            if cgroup_version != "2":
                blockers.append(f"Se requiere cgroups v2; el daemon reporta {cgroup_version}")
        else:
            blockers.append(f"Formato inesperado de docker info: {info.stdout.strip()}")

    if args.engine == "mongodb":
        compatibility = mongodb_compatibility(platform.release(), result["os_release"])
        result["mongodb_kernel_compatibility"] = compatibility
        if not compatibility["compatible"]:
            blockers.append(f"MongoDB bloqueado: {compatibility['rule']}")

    if not blockers and not args.host_only:
        service = args.engine
        listed = run([
            "docker", "ps", "-aq",
            "--filter", f"label=com.docker.compose.project={PROJECT_LABEL}",
            "--filter", f"label=com.docker.compose.service={service}",
        ])
        container_id = listed.stdout.strip().splitlines()[0] if listed.returncode == 0 and listed.stdout.strip() else None
        if not container_id:
            blockers.append(f"Servicio {service} no tiene un contenedor existente; preflight no lo inicia")
        else:
            inspected = run(["docker", "inspect", container_id])
            if inspected.returncode != 0:
                blockers.append(f"docker inspect falló: {inspected.stderr.strip()}")
            else:
                container = json.loads(inspected.stdout)[0]
                state = container["State"]
                host_config = container["HostConfig"]
                expected_cpu = float(expected["cpus"])
                expected_memory = int(expected["memory_mib"]) * 1024 * 1024
                observed_cpu = int(host_config.get("NanoCpus") or 0) / 1_000_000_000
                observed_memory = int(host_config.get("Memory") or 0)
                observed_memory_swap = int(host_config.get("MemorySwap") or 0)
                health = state.get("Health", {}).get("Status", "not_configured")
                labels = container.get("Config", {}).get("Labels") or {}
                checks = [
                    {"name": "container_running", "expected": "running", "observed": state.get("Status"), "ok": state.get("Running") is True},
                    {"name": "healthcheck", "expected": "healthy", "observed": health, "ok": health == "healthy"},
                    {"name": "image_pin", "expected": PINNED_IMAGES[service], "observed": container.get("Config", {}).get("Image"), "ok": container.get("Config", {}).get("Image") == PINNED_IMAGES[service]},
                    {"name": "profile_label", "expected": args.profile, "observed": labels.get("com.u2-benchmark.profile"), "ok": labels.get("com.u2-benchmark.profile") == args.profile},
                    {"name": "cpu_limit", "expected": expected_cpu, "observed": observed_cpu, "ok": math.isclose(observed_cpu, expected_cpu, rel_tol=0, abs_tol=1e-6)},
                    {"name": "memory_limit_bytes", "expected": expected_memory, "observed": observed_memory, "ok": observed_memory == expected_memory},
                    {"name": "memory_swap_total_bytes", "expected": expected_memory, "observed": observed_memory_swap, "ok": observed_memory_swap == expected_memory},
                ]
                if args.engine == "mongodb":
                    configured_ulimits = host_config.get("Ulimits") or []
                    nofile = next((item for item in configured_ulimits if item.get("Name") == "nofile"), None)
                    soft_nofile = nofile.get("Soft") if nofile else None
                    hard_nofile = nofile.get("Hard") if nofile else None
                    checks.extend([
                        {"name": "nofile_soft_limit", "expected_at_least": 65535, "observed": soft_nofile, "ok": isinstance(soft_nofile, int) and soft_nofile >= 65535},
                        {"name": "nofile_hard_limit", "expected_at_least": 65535, "observed": hard_nofile, "ok": isinstance(hard_nofile, int) and hard_nofile >= 65535},
                    ])
                cgroup_path, cgroup_values = read_cgroup_v2(int(state.get("Pid") or 0))
                cgroup_checks: list[dict[str, Any]] = []
                if cgroup_path is not None:
                    try:
                        quota_text, period_text = cgroup_values["cpu.max"].split()
                        cpu_max = "unlimited" if quota_text == "max" else int(quota_text) / int(period_text)
                        cgroup_checks.extend([
                            {"name": "cgroup_cpu.max", "expected": expected_cpu, "observed": cpu_max, "ok": cpu_max != "unlimited" and math.isclose(cpu_max, expected_cpu, rel_tol=0, abs_tol=0.01)},
                            {"name": "cgroup_memory.max", "expected": expected_memory, "observed": int(cgroup_values["memory.max"]), "ok": int(cgroup_values["memory.max"]) == expected_memory},
                            {"name": "cgroup_memory.swap.max", "expected": 0, "observed": int(cgroup_values["memory.swap.max"]), "ok": int(cgroup_values["memory.swap.max"]) == 0},
                        ])
                    except (KeyError, ValueError, ZeroDivisionError) as exc:
                        blockers.append(f"No se pudieron interpretar los controladores cgroups v2: {exc}")
                else:
                    blockers.append("No se pudo resolver el cgroup v2 del PID del contenedor desde /proc")
                all_checks = checks + cgroup_checks
                failed = [check for check in all_checks if not check["ok"]]
                if failed:
                    blockers.extend(f"{check['name']}: esperado {check['expected']}, observado {check['observed']}" for check in failed)
                result.update({
                    "container_id": container_id,
                    "container_state": state.get("Status"),
                    "container_health": health,
                    "host_config": {
                        "nano_cpus": host_config.get("NanoCpus"),
                        "memory_bytes": host_config.get("Memory"),
                        "memory_swap_total_bytes": host_config.get("MemorySwap"),
                    },
                    "cgroup_path": str(cgroup_path) if cgroup_path else None,
                    "cgroup": cgroup_values,
                    "checks": all_checks,
                })

    result["scope"] = "host_only" if args.host_only else "host_and_container"
    result["blockers"] = blockers
    result["ready"] = not blockers
    report = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    print(report, end="")
    if args.output:
        output = args.output if args.output.is_absolute() else ROOT / args.output
        if output.exists():
            print(f"No se sobrescribe evidencia existente: {output}", file=sys.stderr)
            return 2
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
