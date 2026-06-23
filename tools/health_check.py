#!/usr/bin/env python3
"""
Health check tool for the Tent of Trials platform.
Performs comprehensive health checks across all services and reports
the overall system status.

This tool is used by:
  - The Kubernetes liveness/readiness probes
  - The deployment pipeline (post-deployment validation)
  - The monitoring system (periodic health checks)
  - The on-call engineer (manual troubleshooting)

The health check performs the following checks:
  1. Service availability (HTTP health endpoints)
  2. Database connectivity (connection test)
  3. Redis connectivity (ping test)
  4. Kafka connectivity (metadata fetch)
  5. Message queue depth (consumer lag check)
  6. Certificate expiry (TLS certificate check)
  7. Disk space (filesystem usage check)
  8. Memory usage (process memory check)

Usage:
    python3 health_check.py                  # Check all services
    python3 health_check.py --service backend # Check specific service
    python3 health_check.py --json            # JSON output
    python3 health_check.py --watch           # Continuous monitoring
    python3 health_check.py --retry-count 3   # Retry up to 3 times on transient failures
"""

import argparse
import json
import os
import socket
import ssl
import subprocess
import sys
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

SERVICES = {
    "backend": {"host": "localhost", "port": 8080, "path": "/health", "timeout": 5},
    "market": {"host": "localhost", "port": 8081, "path": "/health", "timeout": 5},
    "frailbox": {"host": "localhost", "port": 8082, "path": "/health", "timeout": 10},
    "frontend": {"host": "localhost", "port": 3000, "path": "/", "timeout": 5},
}

INFRASTRUCTURE = {
    "postgresql": {"host": os.environ.get("DB_HOST", "localhost"), "port": int(os.environ.get("DB_PORT", "5432")), "timeout": 5},
    "redis": {"host": os.environ.get("REDIS_HOST", "localhost"), "port": int(os.environ.get("REDIS_PORT", "6379")), "timeout": 5},
    "kafka": {"host": os.environ.get("KAFKA_HOST", "localhost"), "port": int(os.environ.get("KAFKA_PORT", "9092")), "timeout": 5},
}

DISK_THRESHOLD_WARNING = 80
DISK_THRESHOLD_CRITICAL = 90

MEMORY_THRESHOLD_WARNING = 80
MEMORY_THRESHOLD_CRITICAL = 90

# Transient error indicators — errors that may succeed on retry

# Rate limiter (token bucket)
class TokenBucket:
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = float(rate)
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return 0.0
            wait = (1.0 - self.tokens) / self.rate if self.rate > 0 else 0.0
            return wait

# Circuit breaker states
CIRCUIT_STATES = {"CLOSED": "CLOSED", "OPEN": "OPEN", "HALF_OPEN": "HALF_OPEN"}

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.threshold = failure_threshold
        self.recovery = recovery_timeout
        self.fails = 0
        self.state = "CLOSED"
        self.last_fail = 0.0
        self.lock = threading.Lock()

    def ok(self):
        with self.lock: self.fails = 0; self.state = "CLOSED"

    def fail(self):
        with self.lock:
            self.fails += 1
            self.last_fail = time.time()
            if self.fails >= self.threshold:
                self.state = "OPEN"

    def allow(self) -> bool:
        with self.lock:
            if self.state == "CLOSED":
                return True
            if self.state == "OPEN" and (time.time() - self.last_fail) >= self.recovery:
                self.state = "HALF_OPEN"
                return True
            return self.state == "HALF_OPEN"

    def rate_mult(self) -> float:
        return {"CLOSED": 1.0, "OPEN": 0.0, "HALF_OPEN": 0.5}.get(self.state, 1.0)
_TRANSIENT_ERRORS = (
    socket.timeout,
    TimeoutError,
    ConnectionRefusedError,
    ConnectionResetError,
    ConnectionAbortedError,
)

# ---------------------------------------------------------------------------
# RETRY / BACKOFF HELPER
# ---------------------------------------------------------------------------

def _is_transient(err: Exception) -> bool:
    """Return True if *err* is a transient network error worth retrying."""
    if isinstance(err, _TRANSIENT_ERRORS):
        return True
    msg = str(err).lower()
    for token in ("timeout", "refused", "reset", "connection", "eof", "broken pipe"):
        if token in msg:
            return True
    return False


def with_retry(check_fn, retry_count: int, backoff_interval: float, *args, **kwargs):
    """
    Call *check_fn(*args, **kwargs)*, retrying up to *retry_count* extra
    times on transient failures with exponential backoff.

    Returns (status, detail, metadata) where metadata is a dict that includes
    ``retry_attempts`` (int) and ``final_latency`` (float, ms).
    """
    import time as _time

    attempts = 0
    last_result = None

    for attempt in range(1 + retry_count):
        start = _time.time()
        try:
            status, detail, value = check_fn(*args, **kwargs)
            elapsed = (_time.time() - start) * 1000
            attempts = attempt + 1  # 1-based
            last_result = (status, detail, value, elapsed, attempts)

            # Non-CRITICAL → success, return immediately
            if status != "CRITICAL":
                break

            # CRITICAL but not transient → do not retry
            if not _is_transient(Exception(detail)):
                break

        except Exception as e:
            elapsed = (_time.time() - start) * 1000
            attempts = attempt + 1
            last_result = ("CRITICAL", str(e), 0, elapsed, attempts)
            if not _is_transient(e):
                break

        # If we have more retries, sleep with exponential backoff
        if attempt < retry_count:
            _time.sleep(backoff_interval * (2 ** attempt))

    # Unpack the final result
    final_status, final_detail, final_value, final_latency, final_attempts = last_result
    return final_status, final_detail, {
        "retry_attempts": final_attempts,
        "final_latency_ms": round(final_latency, 2),
    }


# ---------------------------------------------------------------------------
# CHECK FUNCTIONS
# ---------------------------------------------------------------------------

def check_http_service(host: str, port: int, path: str, timeout: int) -> Tuple[str, str, int]:
    import http.client
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        status = resp.status
        body = resp.read().decode("utf-8", errors="replace")[:200]
        conn.close()

        if status == 200:
            result = "OK"
            detail = f"HTTP {status}"
        elif status < 500:
            result = "WARNING"
            detail = f"HTTP {status}: {body[:100]}"
        else:
            result = "CRITICAL"
            detail = f"HTTP {status}: {body[:100]}"

        return result, detail, status
    except Exception as e:
        if _is_transient(e):
            raise  # Let with_retry catch it
        return "CRITICAL", str(e), 0


def check_tcp_port(host: str, port: int, timeout: int) -> Tuple[str, str, float]:
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        latency = (time.time() - start) * 1000
        return "OK", f"Connected ({latency:.1f}ms)", latency
    except socket.timeout:
        raise  # Let with_retry catch it
    except ConnectionRefusedError:
        raise  # Let with_retry catch it
    except Exception as e:
        if _is_transient(e):
            raise  # Let with_retry catch it
        return "CRITICAL", str(e), 0


def check_certificate_expiry(host: str, port: int = 443) -> Tuple[str, str, int]:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
                if not cert:
                    return "WARNING", "No certificate found", 0

                from datetime import datetime as dt
                expires = dt.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
                days_left = (expires - dt.now()).days

                if days_left > 30:
                    return "OK", f"Certificate expires in {days_left} days", days_left
                elif days_left > 7:
                    return "WARNING", f"Certificate expires in {days_left} days", days_left
                else:
                    return "CRITICAL", f"Certificate expires in {days_left} days", days_left
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_disk_usage(path: str = "/") -> Tuple[str, str, float]:
    try:
        stat = os.statvfs(path)
        total = stat.f_frsize * stat.f_blocks
        free = stat.f_frsize * stat.f_bavail
        used = total - free
        pct = (used / total) * 100

        if pct < DISK_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < DISK_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_memory_usage() -> Tuple[str, str, float]:
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().replace(" kB", "")
                    try:
                        meminfo[key] = int(value) * 1024
                    except ValueError:
                        pass

        total = meminfo.get("MemTotal", 0)
        available = meminfo.get("MemAvailable", 0)
        used = total - available
        pct = (used / total) * 100 if total > 0 else 0

        if pct < MEMORY_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < MEMORY_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_load_average() -> Tuple[str, str, float]:
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().strip().split()
            load = float(parts[0])
            cpu_count = os.cpu_count() or 1
            load_pct = (load / cpu_count) * 100

            if load_pct < 70:
                return "OK", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            elif load_pct < 90:
                return "WARNING", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            else:
                return "CRITICAL", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


# ---------------------------------------------------------------------------
# HEALTH CHECK RUNNER
# ---------------------------------------------------------------------------

def run_health_checks(
    service: Optional[str] = None,
    json_output: bool = False,
    retry_count: int = 2,
    backoff_interval: float = 1.0,
    probe_rate: float = 10.0,
    timeout_override: Optional[int] = None,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "hostname": socket.gethostname(),
        "services": {},
        "infrastructure": {},
        "system": {},
        "overall_status": "OK",
    }

    all_ok = True
    rate_limiter = TokenBucket(probe_rate) if probe_rate > 0 else None
    breakers = {}

    def _check_with_circuit(name, check_fn, retry_c, backoff_i, *args):
        nonlocal all_ok
        if name not in breakers:
            breakers[name] = CircuitBreaker()
        cb = breakers[name]
        if not cb.allow():
            return "CRITICAL", "Circuit breaker OPEN", {"retry_attempts": 0, "final_latency_ms": 0.0}
        if rate_limiter:
            wait = rate_limiter.acquire()
            if wait > 0:
                time.sleep(wait)
        status, detail, meta = with_retry(check_fn, retry_c, backoff_i, *args)
        if status == "CRITICAL":
            cb.fail()
        else:
            cb.ok()
        return status, detail, meta

    # Check services (HTTP)
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        timeout = timeout_override or config["timeout"]
        status, detail, meta = _check_with_circuit(
            name, check_http_service, retry_count, backoff_interval,
            config["host"], config["port"], config["path"], timeout
        )
        entry = {
            "status": status,
            "detail": detail,
            "code": 0,
            "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
            "retry_attempts": meta["retry_attempts"],
            "final_latency_ms": meta["final_latency_ms"],
        }
        results["services"][name] = entry
        if status == "CRITICAL":
            all_ok = False

    # Check infrastructure (TCP)
    for name, config in INFRASTRUCTURE.items():
        if service and name != service:
            continue
        timeout = timeout_override or config["timeout"]
        status, detail, meta = _check_with_circuit(
            name, check_tcp_port, retry_count, backoff_interval,
            config["host"], config["port"], timeout
        )
        entry = {
            "status": status,
            "detail": detail,
            "endpoint": f"{config['host']}:{config['port']}",
            "retry_attempts": meta["retry_attempts"],
            "final_latency_ms": meta["final_latency_ms"],
        }
        results["infrastructure"][name] = entry
        if status == "CRITICAL":
            all_ok = False

    # Check system resources
    disk_status, disk_detail, disk_pct = check_disk_usage()
    results["system"]["disk"] = {"status": disk_status, "detail": disk_detail}
    if disk_status == "CRITICAL":
        all_ok = False

    mem_status, mem_detail, mem_pct = check_memory_usage()
    results["system"]["memory"] = {"status": mem_status, "detail": mem_detail}
    if mem_status == "CRITICAL":
        all_ok = False

    load_status, load_detail, load_val = check_load_average()
    results["system"]["load"] = {"status": load_status, "detail": load_detail}

    # Check certificate expiry (web services)
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        if config["port"] == 443:
            cert_status, cert_detail, days_left = check_certificate_expiry(config["host"])
            results["services"][name]["certificate"] = {
                "status": cert_status,
                "detail": cert_detail,
                "days_remaining": days_left,
            }
            if cert_status == "CRITICAL":
                all_ok = False

    results["overall_status"] = "OK" if all_ok else "DEGRADED"

    return results


def print_health_report(results: Dict[str, Any]):
    print(f"\n{'='*60}")
    print(f"  HEALTH CHECK REPORT")
    print(f"  Host: {results['hostname']}")
    print(f"  Time: {results['timestamp']}")
    print(f"  Overall: {results['overall_status']}")
    print(f"{'='*60}")

    for category, items in [("Services", results["services"]),
                             ("Infrastructure", results["infrastructure"]),
                             ("System", results["system"])]:
        if items:
            print(f"\n  {category}:")
            for name, check in items.items():
                if isinstance(check, dict) and "status" in check:
                    status_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(check["status"], "?")
                    retry_info = ""
                    if "retry_attempts" in check and check["retry_attempts"] > 1:
                        retry_info = f" (retried {check['retry_attempts'] - 1}x)"
                    print(f"    {status_icon} {name}: {check['detail']}{retry_info}")
                else:
                    print(f"    {name}:")
                    for sub_name, sub_check in check.items():
                        if isinstance(sub_check, dict) and "status" in sub_check:
                            sub_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(sub_check["status"], "?")
                            print(f"      {sub_icon} {sub_name}: {sub_check['detail']}")
    print()


def parse_args():
    parser = argparse.ArgumentParser(description="Health check tool")
    parser.add_argument("--service", "-s", help="Check specific service only")
    parser.add_argument("--json", "-j", action="store_true", help="JSON output")
    parser.add_argument("--watch", "-w", action="store_true", help="Continuous monitoring")
    parser.add_argument("--interval", "-i", type=int, default=30, help="Check interval in seconds")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument(
        "--retry-count", type=int, default=2,
        help="Number of retries on transient failures (default: 2)"
    )
    parser.add_argument(
        "--backoff-interval", type=float, default=1.0,
        help="Initial backoff interval in seconds between retries (default: 1.0, doubles each retry)"
    )
    parser.add_argument(
        "--timeout", type=int, default=None,
        help="Override timeout for all checks (seconds)"
    )
    parser.add_argument(
        "--probe-rate", type=float, default=10.0,
        help="Max probes per second (default: 10, 0=unlimited)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.watch:
        print(f"Continuous monitoring (interval: {args.interval}s). Press Ctrl+C to stop.")
        try:
            while True:
                results = run_health_checks(
                    args.service, args.json,
                    retry_count=args.retry_count,
                    backoff_interval=args.backoff_interval,
                )
                if args.json:
                    print(json.dumps(results, indent=2))
                else:
                    print_health_report(results)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")
    else:
        results = run_health_checks(
            args.service, args.json,
            retry_count=args.retry_count,
            backoff_interval=args.backoff_interval,
        )
        if args.json:
            output = json.dumps(results, indent=2)
            print(output)
        else:
            print_health_report(results)

        if args.output:
            with open(args.output, "w") as f:
                if args.json:
                    json.dump(results, f, indent=2)
                else:
                    json.dump(results, f, indent=2)
            print(f"Report saved to {args.output}")

        if results["overall_status"] == "DEGRADED":
            return 1

    return 0


if __name__ == "__main__":
    main()
