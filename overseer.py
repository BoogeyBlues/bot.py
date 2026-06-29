#!/usr/bin/env python3
"""
overseer.py — Cross-service autonomous guardian.

Runs as its own Railway service. Does NOT manage subprocesses —
Railway restarts each bot service on crash. Overseer's roles:

  1. Health monitor  — polls both bots' /status/api every 60 s, logs status
  2. Strategist      — reads live metrics, calls Claude for trading insights every 2 h
  3. Idea shipper    — Claude proposes targeted code patches, validates + git-pushes every 4 h
  4. Code scanner    — delegates to wraith.py every SCAN_INTERVAL seconds

Required env vars:
  SNIPER_URL          — public base URL of the sniper service
  DRIFT_URL           — public base URL of the drift service
  ANTHROPIC_API_KEY   — enables Claude roles (strategist + idea shipper)
  GITHUB_TOKEN        — enables autonomous git push from idea shipper

Optional env vars:
  GIT_BRANCH          — branch to push improvements to (default: main)
  SCAN_INTERVAL       — seconds between wraith scans (default: 1800)
  STRATEGY_INTERVAL   — seconds between strategy runs (default: 7200)
  IDEA_INTERVAL       — seconds between idea shipping runs (default: 14400)
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
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN", "")
GIT_BRANCH        = os.environ.get("GIT_BRANCH", "main")

SCAN_INTERVAL     = int(os.environ.get("SCAN_INTERVAL",     "1800"))
STRATEGY_INTERVAL = int(os.environ.get("STRATEGY_INTERVAL", "7200"))
IDEA_INTERVAL     = int(os.environ.get("IDEA_INTERVAL",     "14400"))

MODEL = "claude-opus-4-8"

BOTS = [
    {
        "name":       "sniper",
        "health_url": os.environ.get("SNIPER_URL", "").rstrip("/") + "/status/api",
    },
    {
        "name":       "drift",
        "health_url": os.environ.get("DRIFT_URL",  "").rstrip("/") + "/status/api",
    },
]

WATCHED_FILES      = ["bot.py", "drift_bot.py"]
_running           = True
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
    except Exception as exc:
        log("CLAUDE", f"API error: {exc}")
        return None

def _extract_json(text):
    s, e = text.find("{"), text.rfind("}") + 1
    return json.loads(text[s:e]) if s != -1 and e > 0 else {}

# ── 1. Health monitor ─────────────────────────────────────────────────────────
def _health_loop():
    for bot in BOTS:
        if not bot["health_url"].startswith("http"):
            log("HEALTH", f"WARNING: {bot['name'].upper()}_URL not set — health checks disabled for {bot['name']}")
    time.sleep(30)
    while _running:
        for bot in BOTS:
            url = bot["health_url"]
            if not url.startswith("http") or not _HAS_REQUESTS:
                continue
            try:
                r      = _req.get(url, timeout=10)
                status = "healthy" if r.status_code == 200 else f"HTTP {r.status_code}"
                log("HEALTH", f"{bot['name']}: {status}")
            except Exception as exc:
                log("HEALTH", f"{bot['name']}: UNREACHABLE — {exc}")
        time.sleep(60)

# ── Wraith scan ───────────────────────────────────────────────────────────────
def _scan_loop():
    time.sleep(60)
    wraith = os.path.join(BASE_DIR, "wraith.py")
    while _running:
        if not os.path.exists(wraith):
            log("SCAN", "wraith.py not found — skipping")
            time.sleep(SCAN_INTERVAL)
            continue
        log("SCAN", "Invoking wraith")
        try:
            r = subprocess.run(
                [PYTHON, wraith, "--json"],
                capture_output=True, text=True, timeout=120, cwd=BASE_DIR
            )
            if r.stdout.strip():
                try:
                    results  = json.loads(r.stdout)
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
                        log("SCAN", "All files clean")
                except json.JSONDecodeError:
                    log("SCAN", r.stdout.strip()[:400])
            if r.stderr.strip():
                log("SCAN", f"wraith stderr: {r.stderr.strip()[:200]}")
        except subprocess.TimeoutExpired:
            log("SCAN", "wraith timed out")
        except Exception as exc:
            log("SCAN", f"wraith error: {exc}")
        time.sleep(SCAN_INTERVAL)

# ── 2. Strategist ─────────────────────────────────────────────────────────────
def _fetch_metrics():
    metrics = {}
    if not _HAS_REQUESTS:
        return metrics
    for bot in BOTS:
        url = bot["health_url"]
        if not url.startswith("http"):
            continue
        try:
            r = _req.get(url, timeout=10)
            if r.status_code == 200:
                metrics[bot["name"]] = r.json()
        except Exception:
            pass
    return metrics

def _strategy_loop():
    global _strategy_insights
    time.sleep(120)
    while _running:
        log("STRAT", "Fetching metrics from both bots")
        metrics = _fetch_metrics()
        if not metrics:
            log("STRAT", "No live metrics — skipping cycle")
        elif _HAS_CLAUDE:
            system = (
                "You are an expert algorithmic trading strategist for Solana memecoins "
                "and perpetuals trading bots. Analyze the performance metrics from both bots "
                "and propose 3-5 concrete, actionable improvements. Be specific: name exact "
                "parameters, thresholds, or filters to change. "
                "Return JSON only: "
                "{\"insights\": [{\"bot\": \"sniper|drift\", \"priority\": \"HIGH|MED|LOW\", "
                "\"title\": \"...\", \"action\": \"...\"}]}"
            )
            raw = _claude(system, f"Live metrics:\n{json.dumps(metrics, indent=2)}", max_tokens=1200)
            if raw:
                try:
                    insights = _extract_json(raw).get("insights", [])
                    _strategy_insights = insights
                    for ins in insights:
                        log(
                            "STRAT",
                            f"[{ins.get('bot','?')}][{ins.get('priority','?')}] "
                            f"{ins.get('title','')}: {ins.get('action','')}"
                        )
                except Exception as exc:
                    log("STRAT", f"Parse error: {exc} | raw: {raw[:200]}")
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
            log("SHIP", f"Patch rejected — syntax: {err}")
            return False
        with open(fpath, "w") as f:
            f.write(new_src)
        return True
    except Exception as exc:
        log("SHIP", f"Apply error: {exc}")
        return False

def _git_push():
    try:
        subprocess.run(["git", "add"] + WATCHED_FILES, cwd=BASE_DIR, check=True, capture_output=True)
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=BASE_DIR, capture_output=True).returncode == 0:
            log("SHIP", "Nothing to commit")
            return False
        subprocess.run(["git", "config", "user.email", "overseer@bot.py"], cwd=BASE_DIR, capture_output=True)
        subprocess.run(["git", "config", "user.name",  "Overseer"],        cwd=BASE_DIR, capture_output=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        subprocess.run(["git", "commit", "-m", f"overseer: autonomous improvement [{stamp}]"],
                       cwd=BASE_DIR, check=True, capture_output=True)
        push_target = f"HEAD:{GIT_BRANCH}"
        if GITHUB_TOKEN:
            remote_url = subprocess.run(["git", "remote", "get-url", "origin"],
                                        cwd=BASE_DIR, capture_output=True, text=True).stdout.strip()
            if "https://" in remote_url:
                auth_url = remote_url.replace("https://", f"https://{GITHUB_TOKEN}@")
                subprocess.run(["git", "push", auth_url, push_target], cwd=BASE_DIR, check=True, capture_output=True)
                log("SHIP", f"Pushed to {GIT_BRANCH}")
                return True
        subprocess.run(["git", "push", "origin", push_target], cwd=BASE_DIR, check=True, capture_output=True)
        log("SHIP", f"Pushed to {GIT_BRANCH}")
        return True
    except subprocess.CalledProcessError as exc:
        log("SHIP", f"Git error: {exc.stderr.decode(errors='replace')[:300] if exc.stderr else exc}")
        return False
    except Exception as exc:
        log("SHIP", f"Push failed: {exc}")
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
        "Prefer numeric threshold changes or adding simple guards — never refactor. "
        "Return JSON only:\n"
        '{"patches": [{"file": "bot.py|drift_bot.py", "description": "one-line summary", '
        '"old_code": "exact verbatim substring", "new_code": "replacement"}]}\n'
        "old_code must appear EXACTLY ONCE verbatim in the file. "
        "Return {\"patches\": []} if no safe, high-confidence improvement exists."
    )
    sources_text = "\n\n".join(
        f"=== {fn} (first 5000 chars) ===\n{src[:5000]}" for fn, src in sources.items()
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
    except Exception as exc:
        log("SHIP", f"Patch parse error: {exc}")
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
            log("SHIP", "No patches proposed")
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
            log("SHIP", f"{applied} patch(es) applied — pushing to {GIT_BRANCH}")
            _git_push()
        else:
            log("SHIP", "No patches applicable this cycle")

# ── Minimal HTTP server so Railway keeps this service alive ───────────────────
def _health_server():
    from http.server import BaseHTTPRequestHandler, HTTPServer
    port = int(os.environ.get("PORT", "8080"))

    class _H(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b'{"service":"overseer","status":"running"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_):
            pass

    HTTPServer(("0.0.0.0", port), _H).serve_forever()

# ── Signal handling ───────────────────────────────────────────────────────────
def _shutdown(sig, _frame):
    global _running
    _running = False
    log("MGR", "Shutdown received")
    sys.exit(0)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    log("MGR",    f"Overseer online | python={PYTHON}")
    log("CLAUDE",  f"Available: {_HAS_CLAUDE} | model={MODEL if _HAS_CLAUDE else 'N/A — set ANTHROPIC_API_KEY'}")
    log("HEALTH",  f"Sniper URL: {os.environ.get('SNIPER_URL', 'NOT SET')}")
    log("HEALTH",  f"Drift URL:  {os.environ.get('DRIFT_URL',  'NOT SET')}")
    log("MGR",    f"Scan {SCAN_INTERVAL}s | Strategy {STRATEGY_INTERVAL}s | Ideas {IDEA_INTERVAL}s")

    threading.Thread(target=_health_server, daemon=True).start()
    threading.Thread(target=_health_loop,   daemon=True).start()
    threading.Thread(target=_scan_loop,     daemon=True).start()
    threading.Thread(target=_strategy_loop, daemon=True).start()
    threading.Thread(target=_idea_loop,     daemon=True).start()

    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(None, None)
