#!/usr/bin/env python3
"""
overseer.py — 24/7 autonomous bot guardian.

Three roles:
  1. Runtime manager  — supervises bot.py + drift_bot.py, auto-restarts on crash
  2. Strategist       — reads live metrics, calls Claude for trading insights every 2 h
  3. Idea shipper     — Claude proposes targeted code patches, validates + git-pushes every 4 h

Code scanning is delegated to wraith.py (runs as subprocess every SCAN_INTERVAL seconds).
"""
import os, sys, time, signal, threading, subprocess, json, tempfile
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
_strategy_insights = []

# ── Logging ───────────────────────────────────────────────────────────────────
def _ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def log(role, msg):
    print(f"[{_ts()}] [OVERSEER/{role.upper()}] {msg}", flush=True)

# ── Claude helper ─────────────────────────────────────────────────────────────
def _claude(system, user_msg, max_tokens=2048):
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
    s, e = text.find("{"), text.rfind("}") + 1
    return json.loads(text[s:e]) if s != -1 and e > 0 else {}

# ── 1. Runtime manager ────────────────────────────────────────────────────────
def _start_bot(bot):
    proc = subprocess.Popen(
        bot["cmd"], stdout=sys.stdout, stderr=sys.stderr, cwd=BASE_DIR
    )
    with _lock:
        _procs[bot["name"]] = proc
    return proc

def _watch(bot):
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
                r      = _req.get(bot["health_url"], timeout=6)
                status = "healthy" if r.status_code == 200 else f"HTTP {r.status_code}"
                log("MGR", f"{bot['name']}: {status}")
            except Exception as e:
                log("MGR", f"{bot['name']}: UNREACHABLE — {e}")
        time.sleep(60)

def _status_loop():
    while _running:
        time.sleep(300)
        with _lock:
            parts = [
                f"{name}:{'UP pid=' + str(p.pid) if p.poll() is None else 'DOWN'} "
                f"r={_restarts.get(name, 0)}"
                for name, p in _procs.items()
            ]
        log("MGR", " | ".join(parts))

# ── Wraith scan (delegated) ───────────────────────────────────────────────────
def _scan_loop():
    time.sleep(90)
    wraith = os.path.join(BASE_DIR, "wraith.py")
    while _running:
        log("SCAN", "Invoking wraith code scanner")
        try:
            r = subprocess.run(
                [PYTHON, wraith, "--json"],
                capture_output=True, text=True, timeout=120, cwd=BASE_DIR
            )
            if r.stdout.strip():
                try:
                    results = json.loads(r.stdout)
                    any_high = False
                    for fname, findings in results.items():
                        if not findings:
                            log("SCAN", f"{fname}: CLEAN")
                            continue
                        for f in findings:
                            sev  = f.get("severity", "?")
                            desc = f.get("description", "")
                            ln   = f.get("line")
                            loc  = f"{fname}:{ln}" if ln else fname
                            log("SCAN", f"[{sev}] {loc} — {desc}")
                            if sev in ("CRITICAL", "HIGH"):
                                any_high = True
                    if not any_high:
                        log("SCAN", "All files clean (wraith)")
                except json.JSONDecodeError:
                    log("SCAN", r.stdout.strip())
            if r.stderr.strip():
                log("SCAN", f"wraith stderr: {r.stderr.strip()[:200]}")
        except subprocess.TimeoutExpired:
            log("SCAN", "wraith timed out")
        except Exception as e:
            log("SCAN", f"wraith error: {e}")
        time.sleep(SCAN_INTERVAL)

# ── 2. Strategist ─────────────────────────────────────────────────────────────
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
    time.sleep(180)
    while _running:
        log("STRAT", "Fetching metrics for strategy analysis")
        metrics = _fetch_metrics()
        if not metrics:
            log("STRAT", "No live metrics — skipping cycle")
        elif _HAS_CLAUDE:
            system = (
                "You are an expert algorithmic trading strategist for Solana memecoins "
                "and perpetuals trading bots. Analyze the performance metrics and propose "
                "3-5 concrete, actionable improvements. Be specific: name exact parameters, "
                "thresholds, or filters to change. "
                "Return JSON only: "
                "{\"insights\": [{\"priority\": \"HIGH|MED|LOW\", \"title\": \"...\", \"action\": \"...\"}]}"
            )
            raw = _claude(system, f"Live metrics:\n{json.dumps(metrics, indent=2)}", max_tokens=1200)
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

# ── 3. Idea shipper ───────────────────────────────────────────────────────────
def _syntax_ok_src(src):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
        tmp.write(src)
        tmp_path = tmp.name
    r = subprocess.run(
        [PYTHON, "-m", "py_compile", tmp_path], capture_output=True, text=True
    )
    os.unlink(tmp_path)
    return r.returncode == 0, (r.stdout + r.stderr).strip()

def _apply_patch(fpath, old_code, new_code):
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
        log("SHIP", f"Apply error: {e}")
        return False

def _git_push():
    try:
        subprocess.run(
            ["git", "add"] + WATCHED_FILES,
            cwd=BASE_DIR, check=True, capture_output=True
        )
        if subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=BASE_DIR, capture_output=True
        ).returncode == 0:
            log("SHIP", "Nothing to commit")
            return False
        subprocess.run(["git", "config", "user.email", "overseer@bot.py"],
                       cwd=BASE_DIR, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Overseer"],
                       cwd=BASE_DIR, capture_output=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        subprocess.run(
            ["git", "commit", "-m", f"overseer: autonomous improvement [{stamp}]"],
            cwd=BASE_DIR, check=True, capture_output=True
        )
        push_target = f"HEAD:{GIT_BRANCH}"
        if GITHUB_TOKEN:
            remote_url = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                cwd=BASE_DIR, capture_output=True, text=True
            ).stdout.strip()
            if "https://" in remote_url:
                auth_url = remote_url.replace("https://", f"https://{GITHUB_TOKEN}@")
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
        err = e.stderr.decode(errors="replace")[:300] if e.stderr else str(e)
        log("SHIP", f"Git error: {err}")
        return False
    except Exception as e:
        log("SHIP", f"Push failed: {e}")
        return False

def _generate_patches():
    if not _HAS_CLAUDE or not _strategy_insights:
        return []
    sources = {}
    for fname in WATCHED_FILES:
        fpath = os.path.join(BASE_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath) as f:
                sources[fname] = f.read()
    system = (
        "You are an expert Python developer for Solana trading bots. "
        "Given strategy insights and source code, propose 1-2 small, safe code patches. "
        "Only propose changes you are fully confident about. "
        "Prefer numeric threshold changes or simple guards — never refactor. "
        "Return JSON only:\n"
        '{"patches": [{"file": "bot.py|drift_bot.py", "description": "one-line summary", '
        '"old_code": "exact verbatim substring", "new_code": "replacement"}]}\n'
        "old_code must appear EXACTLY ONCE verbatim in the file. "
        "Return {\"patches\": []} if no safe, high-confidence improvement exists."
    )
    sources_text = "\n\n".join(
        f"=== {fn} (first 5000 chars) ===\n{src[:5000]}"
        for fn, src in sources.items()
    )
    raw = _claude(
        system,
        f"Strategy insights:\n{json.dumps(_strategy_insights, indent=2)}\n\n{sources_text}",
        max_tokens=2048,
    )
    if not raw:
        return []
    try:
        return _extract_json(raw).get("patches", [])
    except Exception as e:
        log("SHIP", f"Patch parse error: {e}")
        return []

def _idea_loop():
    time.sleep(360)
    while _running:
        time.sleep(IDEA_INTERVAL)
        if not _HAS_CLAUDE:
            log("SHIP", "ANTHROPIC_API_KEY not set — skipping idea cycle")
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
            log("SHIP", f"Applying: {desc}")
            if _apply_patch(fpath, old_code, new_code):
                log("SHIP", f"Applied: {desc}")
                applied += 1
            else:
                log("SHIP", f"Rejected: {desc}")
        if applied:
            log("SHIP", f"{applied} patch(es) applied — pushing")
            _git_push()
        else:
            log("SHIP", "No patches applicable this cycle")

# ── Signal handling ───────────────────────────────────────────────────────────
def _shutdown(sig, _frame):
    global _running
    _running = False
    log("MGR", "Shutdown — terminating children")
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
                    log("MGR", f"{name}: SIGKILL sent")
            except Exception:
                pass
    sys.exit(0)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    log("MGR",    f"Overseer online | python={PYTHON}")
    log("CLAUDE",  f"Available: {_HAS_CLAUDE} | model={MODEL if _HAS_CLAUDE else 'N/A (set ANTHROPIC_API_KEY)'}")
    log("MGR",    f"Supervising: {[b['name'] for b in BOTS]}")
    log("MGR",    f"Scan every {SCAN_INTERVAL}s | Strategy every {STRATEGY_INTERVAL}s | Ideas every {IDEA_INTERVAL}s")

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
