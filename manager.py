#!/usr/bin/env python3
"""
Production manager — supervises bot.py (sniper) and drift_bot.py (perp trader).
Catches crashes, auto-restarts with exponential backoff, and health-checks both
processes every 60 s. Traps SIGTERM/SIGINT for clean shutdown.
"""
import os
import sys
import time
import signal
import threading
import subprocess

try:
    import requests as _req
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
PYTHON     = sys.executable
PORT       = int(os.environ.get("PORT",       "5000"))
DRIFT_PORT = int(os.environ.get("DRIFT_PORT", "5001"))

BOTS = [
    {
        "name":      "sniper",
        "cmd":       [PYTHON, "bot.py"],
        "health_url": f"http://localhost:{PORT}/status/api",
        "startup_s":  0,          # start immediately
    },
    {
        "name":      "drift",
        "cmd":       [PYTHON, "drift_bot.py"],
        "health_url": f"http://localhost:{DRIFT_PORT}/status/api",
        "startup_s":  3,          # 3-second stagger so ports don't race
    },
]

_procs   = {}          # name -> Popen
_lock    = threading.Lock()
_running = True
_restarts = {}         # name -> count

def ts():
    return time.strftime("%H:%M:%S")

def log(name, msg):
    print(f"[{ts()}] [MGR] [{name.upper()}] {msg}", flush=True)


# ── Bot watcher ───────────────────────────────────────────────────────────────

def _start(bot) -> subprocess.Popen:
    proc = subprocess.Popen(
        bot["cmd"],
        stdout=sys.stdout,
        stderr=sys.stderr,
        cwd=BASE_DIR,
    )
    with _lock:
        _procs[bot["name"]] = proc
    return proc


def _watch(bot):
    """Runs in its own thread. Starts the bot and restarts it if it exits."""
    name    = bot["name"]
    backoff = 5      # seconds before first restart
    max_bo  = 120    # cap backoff at 2 min

    time.sleep(bot.get("startup_s", 0))

    while _running:
        log(name, f"Starting (restart #{_restarts.get(name, 0)})")
        proc = _start(bot)
        rc   = proc.wait()       # blocks until the process exits
        if not _running:
            break
        n = _restarts.get(name, 0) + 1
        _restarts[name] = n
        log(name, f"Exited code={rc} restart#{n} — waiting {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, max_bo)


# ── Health checker ────────────────────────────────────────────────────────────

def _health_loop():
    time.sleep(45)   # give bots time to fully start
    while _running:
        for bot in BOTS:
            if not _HAS_REQUESTS:
                break
            try:
                r = _req.get(bot["health_url"], timeout=6)
                if r.status_code == 200:
                    log(bot["name"], f"healthy ✓")
                else:
                    log(bot["name"], f"health check returned {r.status_code}")
            except Exception as e:
                log(bot["name"], f"health UNREACHABLE — {e}")
        time.sleep(60)


# ── Status summary ────────────────────────────────────────────────────────────

def _status_loop():
    while _running:
        time.sleep(300)   # every 5 min
        lines = []
        with _lock:
            for name, proc in _procs.items():
                alive = proc.poll() is None
                lines.append(f"{name}: {'UP pid={}'.format(proc.pid) if alive else 'DOWN'} restarts={_restarts.get(name,0)}")
        log("MGR", " | ".join(lines))


# ── Signal handling ───────────────────────────────────────────────────────────

def _shutdown(sig, _frame):
    global _running
    _running = False
    log("MGR", "Shutdown signal received — terminating children")
    with _lock:
        for name, proc in _procs.items():
            try:
                proc.terminate()
                log(name, "SIGTERM sent")
            except Exception:
                pass
    # Give bots 8 s to exit gracefully, then force-kill
    time.sleep(8)
    with _lock:
        for name, proc in _procs.items():
            try:
                if proc.poll() is None:
                    proc.kill()
                    log(name, "SIGKILL sent (did not exit in time)")
            except Exception:
                pass
    sys.exit(0)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    log("MGR", f"Production manager online | python={PYTHON}")
    log("MGR", f"Supervising: {[b['name'] for b in BOTS]}")

    threads = []
    for bot in BOTS:
        t = threading.Thread(target=_watch, args=(bot,), daemon=True)
        t.start()
        threads.append(t)

    threading.Thread(target=_health_loop,  daemon=True).start()
    threading.Thread(target=_status_loop,  daemon=True).start()

    # Keep main thread alive so Railway sees a running process
    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(None, None)
