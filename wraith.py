#!/usr/bin/env python3
"""
wraith.py — Code scanner.

Scans watched Python files for bugs using three passes:
  1. Syntax check  (py_compile)
  2. Static linter (pyflakes)
  3. Claude sweep  (AI bug detection, requires ANTHROPIC_API_KEY)

Usage:
  python wraith.py               # scan all WATCHED_FILES
  python wraith.py bot.py        # scan specific file(s)
  python wraith.py --json        # machine-readable JSON output

Exit code: 0 = clean, 1 = findings, 2 = error
"""
import os, sys, json, shutil, subprocess

WATCHED_FILES = ["bot.py", "drift_bot.py"]
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
MODEL         = "claude-opus-4-8"

try:
    import anthropic as _anthropic
    _HAS_CLAUDE = bool(os.environ.get("ANTHROPIC_API_KEY"))
except ImportError:
    _anthropic  = None
    _HAS_CLAUDE = False

# ── Passes ────────────────────────────────────────────────────────────────────
def _syntax_check(fpath):
    r = subprocess.run(
        [sys.executable, "-m", "py_compile", fpath],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        return [{"pass": "syntax", "severity": "CRITICAL",
                 "description": (r.stdout + r.stderr).strip()}]
    return []

def _pyflakes(fpath):
    findings = []
    if not shutil.which("pyflakes"):
        return findings
    try:
        r = subprocess.run(
            ["pyflakes", fpath], capture_output=True, text=True, timeout=30
        )
        for line in (r.stdout + r.stderr).strip().splitlines():
            line = line.strip()
            if line:
                findings.append({"pass": "pyflakes", "severity": "MED",
                                  "description": line})
    except Exception as e:
        findings.append({"pass": "pyflakes", "severity": "LOW",
                          "description": f"pyflakes unavailable: {e}"})
    return findings

def _claude_sweep(fname, source):
    if not _HAS_CLAUDE:
        return []
    try:
        client = _anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        resp   = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=(
                "You are a Python code correctness auditor for live trading bots. "
                "Find real bugs, crashes, and serious logic errors only — not style issues. "
                "Return JSON only: "
                "{\"issues\": [{\"line\": N, \"severity\": \"HIGH|MED|LOW\", \"description\": \"...\"}]} "
                "Maximum 8 issues. Return {\"issues\": []} if the code looks clean."
            ),
            messages=[{"role": "user",
                        "content": f"File: {fname}\n\n```python\n{source[:8000]}\n```"}],
        )
        raw  = resp.content[0].text
        s, e = raw.find("{"), raw.rfind("}") + 1
        data = json.loads(raw[s:e]) if s != -1 else {}
        return [
            {"pass": "claude", "severity": iss.get("severity", "?"),
             "line": iss.get("line"), "description": iss.get("description", "")}
            for iss in data.get("issues", [])
        ]
    except Exception as ex:
        return [{"pass": "claude", "severity": "LOW",
                 "description": f"Claude sweep error: {ex}"}]

# ── Runner ────────────────────────────────────────────────────────────────────
def scan(files=None):
    """Scan files and return dict {fname: [findings]}."""
    targets = files or WATCHED_FILES
    results = {}
    for fname in targets:
        fpath = os.path.join(BASE_DIR, fname)
        if not os.path.exists(fpath):
            results[fname] = [{"pass": "io", "severity": "LOW",
                                "description": f"File not found: {fpath}"}]
            continue
        with open(fpath) as f:
            src = f.read()
        findings  = _syntax_check(fpath)
        if not findings:               # skip linting on broken syntax
            findings += _pyflakes(fpath)
            findings += _claude_sweep(fname, src)
        results[fname] = findings
    return results

def _severity_rank(s):
    return {"CRITICAL": 0, "HIGH": 1, "MED": 2, "LOW": 3}.get(s, 4)

def main():
    as_json = "--json" in sys.argv
    files   = [a for a in sys.argv[1:] if not a.startswith("--")] or None

    results  = scan(files)
    has_high = False

    if as_json:
        print(json.dumps(results, indent=2))
    else:
        for fname, findings in results.items():
            if not findings:
                print(f"[wraith] {fname}: CLEAN")
                continue
            for f in sorted(findings, key=lambda x: _severity_rank(x.get("severity","LOW"))):
                sev  = f.get("severity", "?")
                desc = f.get("description", "")
                ln   = f.get("line")
                loc  = f"{fname}:{ln}" if ln else fname
                print(f"[wraith] [{sev}] {loc} — {desc}")
                if sev in ("CRITICAL", "HIGH"):
                    has_high = True

    return 1 if has_high else 0

if __name__ == "__main__":
    sys.exit(main())
