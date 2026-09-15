#!/usr/bin/env python3
"""Matched evaluation: native Fusion vs relay (legacy) vs relay (durable host).

Design (fixed before running):
- Profiles share the SAME model selector, CLI, permission mode, prompts,
  workspace fixtures and machine; only WINDSURF_API_SERVER_URL differs.
    native   : no relay (Cognition-billed Astra + SWE-2)
    legacy   : relay on 127.0.0.1:8940, FUSION_RELAY_CONTINUATION=legacy
    durable  : experimental host proxy on 127.0.0.1:8951 (durable ledger,
               launcher-provenance bindings, hooks installed)
- Task categories: no-tool arithmetic, single file read, multi-file read,
  exec tool, sidekick delegation, long-context needle. Each task has one
  deterministic expected final line.
- Every task run is followed by a RESUME step (`devin -r <id>`) asking the
  agent to recall its previous answer: the "restart recall" metric.
- REPEATS runs per (profile, task); run order is a seeded random
  interleaving of all (profile, task, repeat) triples so drift affects
  profiles equally. Runs are sequential (no concurrency confound).
- Recorded per run: success (exact expected line present), resume
  success, wall time of each step, CLI exit codes, error text class, and
  (relay profiles only) provider call count / tokens read from the relay's
  own sanitized request log. Nothing from the transcripts is stored beyond
  whether the expected line was present.
Scoring and CIs are computed by summarize.py from bench_results.jsonl.
"""
from __future__ import annotations

import json
import os
import pathlib
import random
import re
import shutil
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
MODEL = "fusion-gpt-6-astra-high-sidekick-swe-2-medium"
CONFIG_SRC = pathlib.Path("/tmp/relay-live-c/config.json")
SEED = 20260915
REPEATS = int(os.environ.get("BENCH_REPEATS", "2"))
TIMEOUT_S = 240

PROFILES = {
    "native": {"env": {}, "unset": ["WINDSURF_API_SERVER_URL"],
               "log": None},
    "legacy": {"env": {"WINDSURF_API_SERVER_URL": None},  # filled at start
               "unset": [],
               "log": pathlib.Path.home() / ".local/share/"
               "fusion-codex-relay-live-a/requests.jsonl",
               "token_dir": pathlib.Path.home() / ".local/share/"
               "fusion-codex-relay-live-a", "port": 8940},
    "durable": {"env": {"WINDSURF_API_SERVER_URL": None},
                "unset": [],
                "log": pathlib.Path.home() / ".local/share/"
                "fusion-codex-relay-live-d/requests.jsonl",
                "env_file": pathlib.Path.home() / ".local/share/"
                "fusion-codex-relay-live-d/host.env"},
}

NEEDLE_LINE = 173


def _fixtures(ws: pathlib.Path) -> None:
    (ws / "f1.txt").write_text("10\n20\n30\n")
    (ws / "f2.txt").write_text("a\nb\nc\nd\n")
    (ws / "words.txt").write_text("one two three four five six seven\n")
    lines = ["line %03d filler text" % i for i in range(1, 301)]
    lines[NEEDLE_LINE - 1] = "line %03d NEEDLE here" % NEEDLE_LINE
    (ws / "long.txt").write_text("\n".join(lines) + "\n")


TASKS = [
    {"id": "arith", "category": "no_tool", "mode": "auto",
     "prompt": "Without any tools, compute 37*43+19 and reply with exactly one line: ANSWER <number>.",
     "expect": "ANSWER 1610"},
    {"id": "read1", "category": "single_tool", "mode": "auto",
     "prompt": "Use the read tool on ./f1.txt (it contains one integer per line), then reply with exactly one line: ANSWER <sum of the integers>. No other tools.",
     "expect": "ANSWER 60"},
    {"id": "read2", "category": "multi_tool", "mode": "auto",
     "prompt": "Use the read tool on ./f1.txt and then on ./f2.txt, then reply with exactly one line: ANSWER <line count of f1> <line count of f2>. No other tools.",
     "expect": "ANSWER 3 4"},
    {"id": "exec", "category": "exec_tool", "mode": "dangerous",
     "prompt": "Run exactly `wc -w words.txt` with the exec tool, then reply with exactly one line: ANSWER <the word count>. No other tools.",
     "expect": "ANSWER 7"},
    {"id": "sidekick", "category": "sidekick", "mode": "dangerous",
     "prompt": "Hand off to your sidekick (run_subagent) exactly one task: \"run `wc -l f2.txt` with exec and report the number\". After it returns, reply with exactly one line: ANSWER <the sidekick's number>. Do nothing else yourself.",
     "expect": "ANSWER 4"},
    {"id": "needle", "category": "long_context", "mode": "auto",
     "prompt": "Use the read tool on ./long.txt (300 lines) and find the single line containing the word NEEDLE. Reply with exactly one line: ANSWER <that line's number as printed at the start of the line, without leading zeros>. No other tools.",
     "expect": "ANSWER %d" % NEEDLE_LINE},
]

RECALL_PROMPT = ("Without any tools, restate the number(s) from your ANSWER "
                 "line earlier in this conversation as exactly one line: "
                 "RECALL <the same number(s)>.")


def _profile_env(name: str) -> dict:
    prof = PROFILES[name]
    env = dict(os.environ)
    for k in prof["unset"]:
        env.pop(k, None)
    if name == "legacy":
        tok = (prof["token_dir"] / "relay-token").read_text().strip()
        env["WINDSURF_API_SERVER_URL"] = \
            "http://127.0.0.1:%d/t/%s" % (prof["port"], tok)
    elif name == "durable":
        line = prof["env_file"].read_text().strip()
        env["WINDSURF_API_SERVER_URL"] = line.split("=", 1)[1]
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _log_len(name: str) -> int:
    log = PROFILES[name]["log"]
    if log is None or not log.exists():
        return 0
    return sum(1 for _ in open(log))


def _log_slice(name: str, start: int) -> dict:
    log = PROFILES[name]["log"]
    if log is None or not log.exists():
        return {"provider_calls": None, "input_tokens": None,
                "output_tokens": None, "errors": None}
    rows = [json.loads(l) for l in open(log).readlines()[start:]]
    chat = [r for r in rows if r.get("rpc") == "GetChatMessage"]
    calls = sum(len(r.get("codex_usage_calls") or []) for r in chat)
    inp = sum((r.get("codex_usage") or {}).get("input_tokens", 0)
              for r in chat if isinstance(r.get("codex_usage"), dict))
    out = sum((r.get("codex_usage") or {}).get("output_tokens", 0)
              for r in chat if isinstance(r.get("codex_usage"), dict))
    errors = [r.get("error_category") for r in chat
              if r.get("error_category") and
              r.get("error_category") != "cancelled"]
    return {"provider_calls": calls, "input_tokens": inp,
            "output_tokens": out, "errors": errors,
            "codex_rows": sum(1 for r in chat if r.get("route") == "codex"),
            "native_rows": sum(1 for r in chat
                               if r.get("route") == "cognition-forward")}


def _run_cli(ws: pathlib.Path, env: dict, args: list, prompt: str,
             mode: str) -> dict:
    env = dict(env, DEVIN_PERMISSION_MODE=mode)
    cmd = ["devin", "--config", str(ws / "config.json"),
           "--respect-workspace-trust", "false", *args, "-p", prompt]
    started = time.monotonic()
    try:
        proc = subprocess.run(cmd, cwd=str(ws), env=env,
                              capture_output=True, text=True,
                              timeout=TIMEOUT_S)
        code, out, err = proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as e:
        code, out, err = -1, (e.stdout or ""), "timeout"
    elapsed = time.monotonic() - started
    err_class = None
    if code != 0:
        m = re.search(r"\(([a-z_]+)\)", err or "")
        err_class = m.group(1) if m else (err or "")[:60].strip()
    return {"exit": code, "seconds": round(elapsed, 3),
            "stdout_lines": len(out.splitlines()),
            "error_class": err_class, "_out": out}


def _session_id(ws: pathlib.Path, env: dict) -> str | None:
    try:
        proc = subprocess.run(["devin", "list", "--format", "json"],
                              cwd=str(ws), env=env, capture_output=True,
                              text=True, timeout=30)
        d = json.loads(proc.stdout)
        d = d if isinstance(d, list) else d.get("sessions", d)
        return d[0]["id"] if d else None
    except Exception:
        return None


def main() -> int:
    out_path = HERE / "bench_results.jsonl"
    root = pathlib.Path("/tmp/relay-bench")
    if root.exists():
        shutil.rmtree(root)
    root.mkdir()
    triples = [(p, t, r) for p in PROFILES for t in range(len(TASKS))
               for r in range(REPEATS)]
    random.Random(SEED).shuffle(triples)
    plan = {"seed": SEED, "repeats": REPEATS, "model": MODEL,
            "order": [(p, TASKS[t]["id"], r) for p, t, r in triples],
            "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (HERE / "bench_plan.json").write_text(json.dumps(plan, indent=1))
    with open(out_path, "a") as fh:
        for n, (profile, ti, rep) in enumerate(triples, 1):
            task = TASKS[ti]
            ws = root / ("%03d-%s-%s-%d" % (n, profile, task["id"], rep))
            ws.mkdir()
            shutil.copy(CONFIG_SRC, ws / "config.json")
            _fixtures(ws)
            env = _profile_env(profile)
            env["DEVIN_MODEL"] = MODEL
            log0 = _log_len(profile)
            step1 = _run_cli(ws, env, [], task["prompt"], task["mode"])
            success = task["expect"] in step1.pop("_out")
            sid = _session_id(ws, env)
            recall = None
            if sid:
                step2 = _run_cli(ws, env, ["-r", sid], RECALL_PROMPT,
                                 "auto")
                expected = task["expect"].replace("ANSWER", "RECALL")
                recall = {"success": expected in step2.pop("_out"),
                          **step2}
            usage = _log_slice(profile, log0)
            row = {"n": n, "profile": profile, "task": task["id"],
                   "category": task["category"], "repeat": rep,
                   "success": success, "step1": step1,
                   "recall": recall, "usage": usage,
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            fh.write(json.dumps(row) + "\n")
            fh.flush()
            print("%3d/%d %-8s %-9s r%d ok=%s recall=%s %.1fs" % (
                n, len(triples), profile, task["id"], rep, success,
                recall and recall["success"], step1["seconds"]),
                flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
