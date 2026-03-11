from __future__ import annotations

import csv
import re
import subprocess
from pathlib import Path
from threading import Lock
from xml.etree import ElementTree as ET

from flask import Flask, jsonify, request

app = Flask(__name__)

ROOT = Path(__file__).resolve().parent.parent
HOSTFILE = ROOT / "hosts.txt"
PLATFORM = ROOT / "platform.xml"
RESULTS = ROOT / "results.csv"
BINARY_MAP = {"ring": ROOT / "ring", "pingpong": ROOT / "pingpong"}
CSV_HEADERS = ["algorithm", "processes", "message_size", "latency", "bandwidth", "time"]
RESULTS_LOCK = Lock()


def parse_int(payload: dict, key: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(payload.get(key, default))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


def ensure_results_file() -> None:
    with RESULTS_LOCK:
        if not RESULTS.exists() or RESULTS.stat().st_size == 0:
            with RESULTS.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(CSV_HEADERS)


def write_hostfile(processes: int) -> None:
    HOSTFILE.write_text("\n".join(f"node{i}" for i in range(processes)) + "\n", encoding="utf-8")


def update_platform(latency: str, bandwidth: str) -> None:
    if not PLATFORM.exists():
        return
    tree = ET.parse(PLATFORM)
    root = tree.getroot()
    changed = False
    for elem in root.iter():
        if "latency" in elem.attrib:
            elem.set("latency", latency)
            changed = True
        if "bandwidth" in elem.attrib:
            elem.set("bandwidth", bandwidth)
            changed = True
    if changed:
        tree.write(PLATFORM, encoding="utf-8", xml_declaration=True)


def parse_execution_time(output: str) -> float | None:
    patterns = [
        r"(?:execution\s+time|elapsed\s+time|time)\s*[=:]\s*([0-9]+(?:\.[0-9]+)?)",
        r"([0-9]+(?:\.[0-9]+)?)\s*(?:s|sec|secs|second|seconds)\b",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, output, flags=re.IGNORECASE)
        if matches:
            return float(matches[-1])
    return None


def append_result(row: dict[str, str | int | float]) -> None:
    ensure_results_file()
    with RESULTS_LOCK:
        with RESULTS.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([row[h] for h in CSV_HEADERS])


def run_one(payload: dict) -> tuple[dict, int]:
    algorithm = str(payload.get("algorithm", "ring")).strip().lower()
    binary = BINARY_MAP.get(algorithm)
    if binary is None:
        return {"error": f"unsupported algorithm: {algorithm}"}, 400
    if not binary.exists():
        return {"error": f"binary not found: {binary.name}"}, 500

    try:
        processes = parse_int(payload, "number_of_processes", 4)
        message_size = parse_int(payload, "message_size", 1024)
        iterations = parse_int(payload, "iterations", 10)
    except ValueError as exc:
        return {"error": str(exc)}, 400

    latency = str(payload.get("network_latency", "10us")).strip() or "10us"
    bandwidth = str(payload.get("network_bandwidth", "10GBps")).strip() or "10GBps"

    write_hostfile(processes)
    update_platform(latency, bandwidth)
    cmd = [
        "smpirun",
        "-np",
        str(processes),
        "-platform",
        str(PLATFORM),
        "-hostfile",
        str(HOSTFILE),
        str(binary),
        str(message_size),
        str(iterations),
    ]

    try:
        completed = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        return {"error": "smpirun not found in PATH"}, 500
    except subprocess.CalledProcessError as exc:
        return {"error": "simulation failed", "stdout": exc.stdout, "stderr": exc.stderr, "returncode": exc.returncode}, 500

    output = f"{completed.stdout or ''}\n{completed.stderr or ''}"
    exec_time = parse_execution_time(output)
    append_result(
        {
            "algorithm": algorithm,
            "processes": processes,
            "message_size": message_size,
            "latency": latency,
            "bandwidth": bandwidth,
            "time": exec_time if exec_time is not None else "",
        }
    )
    return {"ok": True, "algorithm": algorithm, "execution_time": exec_time}, 200


@app.post("/run_simulation")
def run_simulation():
    body, code = run_one(request.get_json(silent=True) or {})
    return jsonify(body), code


@app.post("/run_batch")
def run_batch():
    payload = request.get_json(silent=True) or {}
    try:
        start = parse_int(payload, "process_range_start", 2)
        end = parse_int(payload, "process_range_end", 8)
        step = parse_int(payload, "step", 2)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if end < start:
        return jsonify({"error": "process_range_end must be >= process_range_start"}), 400

    runs = []
    for p in range(start, end + 1, step):
        run_payload = dict(payload)
        run_payload["number_of_processes"] = p
        body, code = run_one(run_payload)
        if code != 200:
            return jsonify({"error": "batch failed", "at_processes": p, "details": body}), code
        runs.append({"processes": p, "execution_time": body.get("execution_time")})
    return jsonify({"ok": True, "runs": runs})


@app.get("/results")
def get_results():
    ensure_results_file()
    with RESULTS.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return jsonify(rows)


if __name__ == "__main__":
    ensure_results_file()
    app.run(host="0.0.0.0", port=5000, debug=True)
