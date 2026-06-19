#!/usr/bin/env python3
"""
wraith.py — Autonomous 24/7 bot guardian.

Four roles in one process:
  1. Runtime manager  — supervises bot.py + drift_bot.py, auto-restarts on crash
  2. Code scanner     — pyflakes + Claude bug sweep every SCAN_INTERVAL seconds
  3. Strategist       — reads live metrics, calls Claude for trading insights
  4. Idea shipper     — Claude proposes targeted patches, validates + git-pushes
"""
import os, sys, time, signal, threading, subprocess, json, shutil, tempfile
from datetime import datetime, timezone

try:
    import requests as _req
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    import anthropic as _anthropic
    _HAS_CLAUDE = bool(os.environ.get("ANTHROPIC_API_KEY"))
except ImportError:
    _anthropic  = None
    _HAS_CLAUDE = False

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
PYTHON            = sys.executable
PORT              = int(os.environ.get("PORT",       "5000"))
DRIFT_PORT        = int(os.environ.get("DRIFT_PORT", "5001"))
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN", "")
GIT_BRANCH        = os.environ.get("GIT_BRANCH", "main")

SCAN_INTERVAL     = int(os.environ.get("SCAN_INTERVAL",     "1800"))   # 30 min
STRATEGY_INTERVAL = int(os.environ.get("STRATEGY_INTERVAL", "7200"))   # 2 hours
IDEA_INTERVAL     = int(os.environ.get("IDEA_INTERVAL",     "14400"))  # 4 hours

MODEL = "claude-opus-4-8"

BOTS = [
    {
        "name":       "sniper",
        "cmd":        [PYTHON, "bot.py"],
        "health_url": f"http://localhost:{PORT}/status/api",
        "startup_s":  0,
    },
    {
        "name":       "drift",
        "cmd":        [PYTHON, "drift_bot.py"],
        "health_url": f"http://localhost:{DRIFT_PORT}/status/api",
        "startup_s":  3,
    },
]
WATCHED_FILES = ["bot.py", "drift_bot.py"]

_procs             = {}
_lock              = threading.Lock()
_running           = True
_restarts          = {}
_strategy_insights = []   # written by strategist, read by idea shipper

# ── Logging ───────────────────────────────────────────────────────────────────
def _ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def log(role, msg):
    print(f"[{_ts()}] [WRAITH/{role.upper()}] {msg}", flush=True)

# ── Claude helper ─────────────────────────────────────────────────────────────
def _claude(system, user_msg, max_tokens=2048):
    """Call Claude; returns text or None on error."""
    if not _HAS_CLAUDE:
        return None
    try:
        client = _anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        resp   = client.messages.create(
            model=MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        return resp.content[0].text
    except Exception as e:
        log("CLAUDE", f"API error: {e}")
        return None

def _extract_json(text):
    """Extract first JSON object from a text response."""
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start == -1 or end == 0:
        return {}
    return json.loads(text[start:end])

# ── 1. Runtime manager ────────────────────────────────────────────────────────
def _start_bot(bot) -> subprocess.Popen:
    proc = subprocess.Popen(
        bot["cmd"], stdout=sys.stdout, stderr=sys.stderr, cwd=BASE_DIR
    )
    with _lock:
        _procs[bot["name"]] = proc
    return proc

def _watch(bot):
    """Supervisor thread: start bot and restart on exit."""
    name    = bot["name"]
    backoff = 5
    max_bo  = 120
    time.sleep(bot.get("startup_s", 0))
    while _running:
        log("MGR", f"Starting {name} (restart #{_restarts.get(name, 0)})")
        proc = _start_bot(bot)
        rc   = proc.wait()
        if not _running:
            break
        n = _restarts.get(name, 0) + 1
        _restarts[name] = n
        log("MGR", f"{name} exited code={rc} restart#{n} — waiting {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, max_bo)

def _health_loop():
    time.sleep(45)
    while _running:
        for bot in BOTS:
            if not _HAS_REQUESTS:
                break
            try:
                r = _req.get(bot["health_url"], timeout=6)
                status = "healthy" if r.status_code == 200 else f"HTTP {r.status_code}"
                log("MGR", f"{bot['name']}: {status}")
            except Exception as e:
                log("MGR", f"{bot['name']}: UNREACHABLE — {e}")
        time.sleep(60)

def _status_loop():
    while _running:
        time.sleep(300)
        with _lock:
            parts = []
            for name, proc in _procs.items():
                alive = proc.poll() is None
                parts.append(
                    f"{name}:{'UP pid=' + str(proc.pid) if alive else 'DOWN'} "
                    f"r={_restarts.get(name, 0)}"
                )
        log("MGR", " | ".join(parts))

# ── 2. Code scanner ───────────────────────────────────────────────────────────
def _syntax_ok(fpath):
    r = subprocess.run(
        [PYTHON, "-m", "py_compile", fpath],
        capture_output=True, text=True
    )
    return r.returncode == 0, (r.stdout + r.stderr).strip()

def _run_pyflakes(fpath):
    if not shutil.which("pyflakes"):
        return ""
    try:
        r = subprocess.run(
            ["pyflakes", fpath], capture_output=True, text=True, timeout=30
        )
        return (r.stdout + r.stderr).strip()
    except Exception:
        return ""

def _scan_with_claude(fname, source):
    system = (
        "You are a Python code correctness auditor for trading bots. "
        "Find real bugs, crashes, or serious logic errors only — not style issues. "
        "Return JSON only: "
        "{\"issues\": [{\"line\": N, \"severity\": \"HIGH|MED|LOW\", \"description\": \"...\"}]} "
        "Maximum 5 issues. Return {\"issues\": []} if the code looks clean."
    )
    user = f"File: {fname}\n\n```python\n{source[:6000]}\n```"
    raw  = _claude(system, user, max_tokens=800)
    if not raw:
        return []
    try:
        return _extract_json(raw).get("issues", [])
    except Exception:
        return []

def _scan_loop():
    time.sleep(90)   # give bots time to start
    while _running:
        log("SCAN", "Starting code scan")
        all_clean = True
        for fname in WATCHED_FILES:
            fpath = os.path.join(BASE_DIR, fname)
            if not os.path.exists(fpath):
                continue
            ok, err = _syntax_ok(fpath)
            if not ok:
                log("SCAN", f"[SYNTAX] {fname}: {err}")
                all_clean = False
                continue
            pf = _run_pyflakes(fpath)
            if pf:
                for line in pf.splitlines():
                    log("SCAN", f"[PYFLAKES] {line}")
                all_clean = False
            if _HAS_CLAUDE:
                with open(fpath) as f:
                    src = f.read()
                for iss in _scan_with_claude(fname, src):
                    sev  = iss.get("severity", "?")
                    desc = iss.get("description", "")
                    ln   = iss.get("line", "?")
                    log("SCAN", f"[{sev}] {fname}:{ln} — {desc}")
                    if sev == "HIGH":
                        all_clean = False
        if all_clean:
            log("SCAN", "All files clean")
        time.sleep(SCAN_INTERVAL)

# ── 3. Strategist ─────────────────────────────────────────────────────────────
def _fetch_metrics():
    metrics = {}
    if not _HAS_REQUESTS:
        return metrics
    for bot in BOTS:
        try:
            r = _req.get(bot["health_url"], timeout=8)
            if r.status_code == 200:
                metrics[bot["name"]] = r.json()
        except Exception:
            pass
    return metrics

def _strategy_loop():
    global _strategy_insights
    time.sleep(180)   # wait for bots to stabilize
    while _running:
        log("STRAT", "Fetching metrics for strategy analysis")
        metrics = _fetch_metrics()
        if not metrics:
            log("STRAT", "No live metrics available — skipping this cycle")
        elif _HAS_CLAUDE:
            system = (
                "You are an expert algorithmic trading strategist for Solana memecoins "
                "and perpetuals trading bots. Analyze the performance metrics and propose "
                "3-5 concrete, actionable improvements. Be specific: name exact parameters "
                "to change, thresholds to adjust, or filters to add. "
                "Return JSON only: "
                "{\"insights\": [{\"priority\": \"HIGH|MED|LOW\", \"title\": \"...\", \"action\": \"...\"}]}"
            )
            user = f"Live bot metrics:\n{json.dumps(metrics, indent=2)}"
            raw  = _claude(system, user, max_tokens=1200)
            if raw:
                try:
                    insights = _extract_json(raw).get("insights", [])
                    _strategy_insights = insights
                    for ins in insights:
                        log(
                            "STRAT",
                            f"[{ins.get('priority','?')}] {ins.get('title','')}: "
                            f"{ins.get('action','')}"
                        )
                except Exception as e:
                    log("STRAT", f"Parse error: {e} | raw: {raw[:200]}")
        time.sleep(STRATEGY_INTERVAL)

# ── 4. Idea shipper ───────────────────────────────────────────────────────────
def _syntax_ok_src(src):
    """Check syntax of an in-memory source string."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
        tmp.write(src)
        tmp_path = tmp.name
    ok, err = _syntax_ok(tmp_path)
    os.unlink(tmp_path)
    return ok, err

def _apply_patch(fpath, old_code, new_code):
    """Replace old_code with new_code in file. Validates syntax first."""
    try:
        with open(fpath) as f:
            src = f.read()
        if old_code not in src:
            log("SHIP", f"Patch target not found in {os.path.basename(fpath)}")
            return False
        new_src = src.replace(old_code, new_code, 1)
        ok, err = _syntax_ok_src(new_src)
        if not ok:
            log("SHIP", f"Patch rejected — syntax invalid: {err}")
            return False
        with open(fpath, "w") as f:
            f.write(new_src)
        return True
    except Exception as e:
        log("SHIP", f"Patch apply error: {e}")
        return False

def _git_push():
    """Stage watched files, commit changes, and push. Returns True on success."""
    try:
        subprocess.run(
            ["git", "add"] + WATCHED_FILES,
            cwd=BASE_DIR, check=True, capture_output=True
        )
        r = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=BASE_DIR, capture_output=True
        )
        if r.returncode == 0:
            log("SHIP", "Nothing to commit")
            return False
        subprocess.run(
            ["git", "config", "user.email", "wraith@bot.py"],
            cwd=BASE_DIR, capture_output=True
        )
        subprocess.run(
            ["git", "config", "user.name", "Wraith"],
            cwd=BASE_DIR, capture_output=True
        )
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        subprocess.run(
            ["git", "commit", "-m", f"wraith: autonomous improvement [{stamp}]"],
            cwd=BASE_DIR, check=True, capture_output=True
        )
        # Build push command — inject GITHUB_TOKEN if available
        push_target = f"HEAD:{GIT_BRANCH}"
        if GITHUB_TOKEN:
            remote_url = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                cwd=BASE_DIR, capture_output=True, text=True
            ).stdout.strip()
            if "https://" in remote_url:
                auth_url = remote_url.replace(
                    "https://", f"https://{GITHUB_TOKEN}@"
                )
                subprocess.run(
                    ["git", "push", auth_url, push_target],
                    cwd=BASE_DIR, check=True, capture_output=True
                )
                log("SHIP", f"Pushed to {GIT_BRANCH} (token auth)")
                return True
        subprocess.run(
            ["git", "push", "origin", push_target],
            cwd=BASE_DIR, check=True, capture_output=True
        )
        log("SHIP", f"Pushed to {GIT_BRANCH}")
        return True
    except subprocess.CalledProcessError as e:
        err_text = e.stderr.decode(errors="replace")[:300] if e.stderr else str(e)
        log("SHIP", f"Git error: {err_text}")
        return False
    except Exception as e:
        log("SHIP", f"Push failed: {e}")
        return False

def _generate_patches():
    """Ask Claude to propose code patches based on strategy insights."""
    if not _HAS_CLAUDE or not _strategy_insights:
        return []
    sources = {}
    for fname in WATCHED_FILES:
        fpath = os.path.join(BASE_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath) as f:
                sources[fname] = f.read()
    system = (
        "You are an expert Python developer working on Solana trading bots. "
        "Given strategy insights and bot source code, propose 1-2 small, safe code patches. "
        "Only propose changes you are fully confident about. "
        "Prefer changes to numeric thresholds, timeout values, or adding simple guards — "
        "never refactor or restructure code. "
        "Return JSON only:\n"
        '{"patches": [{"file": "bot.py|drift_bot.py", "description": "one-line summary", '
        '"old_code": "exact verbatim substring from the file", '
        '"new_code": "replacement for that substring"}]}\n'
        "old_code must be a verbatim substring that appears EXACTLY ONCE in the file. "
        "Return {\"patches\": []} if no safe, high-confidence improvement exists."
    )
    insights_text = json.dumps(_strategy_insights, indent=2)
    sources_text  = "\n\n".join(
        f"=== {fname} (first 5000 chars) ===\n{src[:5000]}"
        for fname, src in sources.items()
    )
    user = f"Strategy insights:\n{insights_text}\n\nSource files:\n{sources_text}"
    raw  = _claude(system, user, max_tokens=2048)
    if not raw:
        return []
    try:
        return _extract_json(raw).get("patches", [])
    except Exception as e:
        log("SHIP", f"Failed to parse patches: {e}")
        return []

def _idea_loop():
    time.sleep(360)   # wait for first strategy cycle to populate insights
    while _running:
        time.sleep(IDEA_INTERVAL)
        if not _HAS_CLAUDE:
            log("SHIP", "Claude unavailable — ANTHROPIC_API_KEY not set")
            continue
        log("SHIP", "Generating improvement patches")
        patches = _generate_patches()
        if not patches:
            log("SHIP", "No patches proposed this cycle")
            continue
        applied = 0
        for patch in patches:
            fname    = patch.get("file", "")
            desc     = patch.get("description", "")
            old_code = patch.get("old_code", "")
            new_code = patch.get("new_code", "")
            if not fname or not old_code or not new_code or old_code == new_code:
                continue
            fpath = os.path.join(BASE_DIR, fname)
            if not os.path.exists(fpath):
                log("SHIP", f"File not found: {fname}")
                continue
            log("SHIP", f"Applying patch: {desc}")
            if _apply_patch(fpath, old_code, new_code):
                log("SHIP", f"Applied: {desc}")
                applied += 1
            else:
                log("SHIP", f"Rejected: {desc}")
        if applied:
            log("SHIP", f"{applied} patch(es) applied — pushing")
            _git_push()
        else:
            log("SHIP", "No patches were applicable this cycle")

# ── Signal handling ───────────────────────────────────────────────────────────
def _shutdown(sig, _frame):
    global _running
    _running = False
    log("MGR", "Shutdown signal — terminating children")
    with _lock:
        for name, proc in _procs.items():
            try:
                proc.terminate()
                log("MGR", f"{name}: SIGTERM sent")
            except Exception:
                pass
    time.sleep(8)
    with _lock:
        for name, proc in _procs.items():
            try:
                if proc.poll() is None:
                    proc.kill()
                    log("MGR", f"{name}: SIGKILL sent (did not exit in time)")
            except Exception:
                pass
    sys.exit(0)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    log("MGR",   f"Wraith online | python={PYTHON}")
    log("CLAUDE", f"Available: {_HAS_CLAUDE} | model={MODEL if _HAS_CLAUDE else 'N/A (set ANTHROPIC_API_KEY)'}")
    log("MGR",   f"Supervising: {[b['name'] for b in BOTS]}")
    log("MGR",   f"Scan every {SCAN_INTERVAL}s | Strategy every {STRATEGY_INTERVAL}s | Ideas every {IDEA_INTERVAL}s")

    for bot in BOTS:
        threading.Thread(target=_watch, args=(bot,), daemon=True).start()

    threading.Thread(target=_health_loop,   daemon=True).start()
    threading.Thread(target=_status_loop,   daemon=True).start()
    threading.Thread(target=_scan_loop,     daemon=True).start()
    threading.Thread(target=_strategy_loop, daemon=True).start()
    threading.Thread(target=_idea_loop,     daemon=True).start()

    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(None, None)
