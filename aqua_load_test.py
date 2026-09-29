#!/usr/bin/env python3
"""Tiered load test for Aqua, Code Ocean's AI assistant, and the model backend behind it.

A tier is N simulated users. Each simulated user is a worker that sends prompts
to Aqua through the aqua CLI. Workers start --stagger seconds apart. By default
each worker sends --runs-per-user prompts and stops; with --hold, workers keep
sending prompts back to back for that many seconds instead.

After each tier the error rate is checked. The test moves to the next tier
until a tier's error rate reaches --stop-error-rate. It also stops mid-tier if
the rolling error rate reaches --abort-error-rate (default: the same value).

Every run is logged to runs.jsonl with its Aqua session_id, so runs can be
matched to server-side logs or analytics afterwards.

Standard library only, Python 3.8+. If the aqua CLI is not on PATH it is
downloaded from the deployment itself.
"""

import argparse
import asyncio
import collections
import hashlib
import json
import math
import os
import platform
import re
import shutil
import signal
import stat
import sys
import tarfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_SUFFIX = "Run this prompt start to end, without asking for questions."
TOKEN_ENV_NAMES = ("CODEOCEAN_TOKEN", "CODEOCEAN_API_TOKEN", "CO_API_TOKEN", "API_TOKEN")
TOKEN_RE = re.compile(r"cop_[A-Za-z0-9_\-]+")
REPLY_KEYS = ("result", "reply", "response", "answer", "text", "message", "content", "output")

# Matched in order against the CLI's stderr (lower-cased); the first match wins.
ERROR_PATTERNS = [
    ("auth", r"\b40[13]\b|unauthori[sz]ed|forbidden|invalid (api )?(key|token)|authenticat"),
    ("throttle", r"\b429\b|throttl|too many requests|rate.?limit"),
    ("session_busy", r"\b409\b|conflict|already (running|in progress)"),
    ("upstream_5xx", r"\b50[0234]\b|internal server error|bad gateway|service unavailable"),
    ("timeout", r"timed? ?out|timeout|deadline exceeded"),
    ("stream", r"interrupt|premature|unexpected eof|connection (reset|refused|closed)|broken pipe"),
    ("form", r"clarif|interactive form|ask_user"),
    ("empty_reply", r"empty (reply|response)"),
]

def log(msg):
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def pct(values, q):
    if not values:
        return None
    v = sorted(values)
    return v[max(1, math.ceil(q / 100.0 * len(v))) - 1]


def fmt_s(x):
    return "-" if x is None else f"{x:.0f}s"


def fmt_pct(x):
    return "-" if x is None else f"{x:.1%}"


def fmt_tokens(x):
    return f"{x / 1e6:.1f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


async def wait_event(event, timeout):
    """Sleep up to `timeout` seconds, waking early if `event` is set."""
    if timeout > 0 and not event.is_set():
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
    return event.is_set()


def parse_stdout(raw):
    """Return (session_id, reply_text, json_keys) from `aqua -json` output."""
    text = raw.decode("utf-8", "replace").strip()
    obj = None
    for candidate in [text] + text.splitlines()[::-1]:
        try:
            obj = json.loads(candidate)
            break
        except ValueError:
            continue
    if not isinstance(obj, dict):
        return None, text, None
    reply = next((obj[k] for k in REPLY_KEYS if isinstance(obj.get(k), str)), text)
    return obj.get("session_id") or obj.get("sessionId"), reply, sorted(obj)


def classify(code, timed_out, killed, stderr, reply):
    """Return None for a good run, otherwise a short error class."""
    if timed_out:
        return "client_timeout"
    if killed:
        return "killed"
    if code == 0 and reply.strip():
        return None
    low = stderr.lower()
    for name, pattern in ERROR_PATTERNS:
        if re.search(pattern, low):
            return name
    return "empty_reply" if code == 0 else "other"


def load_accounts(a):
    """Return [(label, token)]. Labels are env var names or file labels, never token values."""
    accounts = []
    if a.tokens:
        path = Path(a.tokens)
        if path.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            log(f"warning: {path} is readable by other users; run: chmod 600 {path}")
        for line in path.read_text().splitlines():
            parts = line.split()
            if not parts or parts[0].startswith("#"):
                continue
            label, token = (parts[0], parts[1]) if len(parts) > 1 else (f"user{len(accounts) + 1}", parts[0])
            accounts.append((label, token))
    elif a.token_envs:
        for name in (n.strip() for n in a.token_envs.split(",") if n.strip()):
            if not os.environ.get(name):
                sys.exit(f"environment variable {name} is not set")
            accounts.append((name, os.environ[name]))
    else:
        name = next((n for n in TOKEN_ENV_NAMES if os.environ.get(n)), None)
        name = name or next((n for n, v in sorted(os.environ.items()) if v.startswith("cop_")), None)
        if name:
            accounts.append((name, os.environ[name]))
    if not accounts:
        sys.exit("No Code Ocean API token found. Attach one to the capsule as a secret "
                 "(Settings > Credentials), export CODEOCEAN_TOKEN, or pass --tokens FILE.")
    return accounts


def load_prompts(path, name):
    sets = json.loads(Path(path).read_text())
    prompts = sets.get(name)
    if isinstance(prompts, str):
        prompts = [prompts]
    if not prompts:
        sys.exit(f"prompt set '{name}' not found in {path} (have: {', '.join(sets)})")
    prompts = [str(q).strip() for q in prompts]
    if any(not q or q.startswith("-") for q in prompts):
        sys.exit(f"prompts in set '{name}' must be non-empty and must not start with '-' (the CLI would read them as flags)")
    return prompts


def ensure_aqua(a):
    """Return a path to the aqua CLI, downloading it from the deployment if needed."""
    if a.aqua:
        if not (shutil.which(a.aqua) or Path(a.aqua).is_file()):
            sys.exit(f"aqua CLI not found at {a.aqua}")
        return shutil.which(a.aqua) or str(Path(a.aqua).resolve())
    found = shutil.which("aqua")
    if found:
        return found
    arch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine().lower())
    system = platform.system().lower()
    if not arch or system not in ("linux", "darwin"):
        sys.exit(f"no aqua CLI build for {system}/{platform.machine()}")
    dest = Path(a.install_dir)
    dest.mkdir(parents=True, exist_ok=True)
    name = f"aqua.{system}.{arch}.tar.gz"
    sources = [f"{a.domain}/downloads/aqua-cli/{name}"] + ([] if a.no_public_cli else [f"https://get.codeocean.com/aqua-cli/{name}"])
    for url in sources:
        if not url.startswith("https://"):
            log(f"not downloading the CLI over plain HTTP: {url}")
            continue
        try:
            log(f"installing aqua CLI from {url}")
            archive = dest / name
            with urllib.request.urlopen(url, timeout=120) as resp, open(archive, "wb") as f:
                shutil.copyfileobj(resp, f)
            with tarfile.open(archive) as tar:
                member = next(m for m in tar.getmembers() if m.isfile() and Path(m.name).name == "aqua")
                with tar.extractfile(member) as src, open(dest / "aqua", "wb") as out:
                    shutil.copyfileobj(src, out)
            (dest / "aqua").chmod(0o755)
            archive.unlink()
            digest = hashlib.sha256((dest / "aqua").read_bytes()).hexdigest()
            if a.aqua_sha256 and digest != a.aqua_sha256.lower():
                (dest / "aqua").unlink()
                sys.exit(f"downloaded aqua CLI has sha256 {digest}, expected {a.aqua_sha256}; not running it")
            log(f"installed aqua CLI sha256 {digest}")
            return str(dest / "aqua")
        except Exception as e:  # try the next source
            hint = " (private CA? set SSL_CERT_FILE to the CA bundle)" if "CERTIFICATE" in str(e).upper() else ""
            log(f"could not install from {url}: {e}{hint}")
    sys.exit("could not install the aqua CLI; put it on PATH or pass --aqua")


def raise_fd_limit(needed):
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = needed if hard == resource.RLIM_INFINITY else min(needed, hard)
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < needed:
            log(f"warning: open-file limit is below {needed}; the largest tiers may fail to start processes")
    except (ImportError, ValueError, OSError) as e:
        log(f"warning: could not raise the open-file limit: {e}")


def tier_estimate(users, a):
    ramp = (users - 1) * a.stagger
    if a.hold:
        runs = users * (a.hold + ramp / 2.0) / (a.assume_run_s + a.think)
        wall = ramp + a.hold + a.assume_run_s
    else:
        runs = users * a.runs_per_user
        wall = ramp + a.runs_per_user * (a.assume_run_s + a.think)
    return ramp, runs, wall, runs * a.assume_run_s * a.assume_tok_s


def print_plan(tiers, a):
    mode = f"each user runs for {a.hold:.0f}s" if a.hold else f"{a.runs_per_user} run(s) per user"
    print(f"\nPlan: tiers {', '.join(map(str, tiers))} | start a user every {a.stagger:g}s | {mode} | "
          f"stop once a tier's error rate reaches {a.stop_error_rate:.0%}")
    print(f"Estimates assume ~{a.assume_run_s:.0f}s per run at ~{a.assume_tok_s:.0f} output tokens/s.\n")
    print(f"{'users':>7} {'ramp':>7} {'runs':>7} {'time':>8} {'output tokens':>14}")
    total_runs = total_wall = total_tokens = 0
    for users in tiers:
        ramp, runs, wall, tokens = tier_estimate(users, a)
        print(f"{users:>7} {ramp:>6.0f}s {runs:>7.0f} {wall / 60:>7.0f}m {fmt_tokens(tokens):>14}")
        total_runs, total_wall, total_tokens = total_runs + runs, total_wall + wall + a.cooldown, total_tokens + tokens
    print(f"{'total':>7} {'':>7} {total_runs:>7.0f} {total_wall / 60:>7.0f}m {fmt_tokens(total_tokens):>14}\n")


class TierState:
    def __init__(self, users):
        self.users = users
        self.records = []
        self.launched = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.stop = asyncio.Event()
        self.abort_reason = None
        self.finished = False


class LoadTest:
    def __init__(self, a, tiers, accounts, prompts, aqua, out):
        self.a, self.tiers, self.accounts, self.prompts, self.aqua, self.out = a, tiers, accounts, prompts, aqua, out
        self.procs = set()
        self.tier = None
        self.fatal = False
        self.killing = False
        self.interrupts = 0
        self.stop_all = asyncio.Event()
        self.json_keys_shown = False
        self.summaries = []
        self.runs_file = open(out / "runs.jsonl", "a", buffering=1)

    # ---- one aqua invocation -------------------------------------------------

    async def invoke(self, token, prompt):
        """Run the CLI once. Returns (exit_code, stdout, stderr, timed_out)."""
        env = dict(os.environ, CODEOCEAN_DOMAIN=self.a.domain, CODEOCEAN_TOKEN=token)
        try:
            proc = await asyncio.create_subprocess_exec(
                self.aqua, "-json", prompt, env=env, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
        except OSError as e:
            return -1, b"", str(e).encode(), False
        self.procs.add(proc)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), self.a.run_timeout)
            return proc.returncode, out, err, False
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return proc.returncode, b"", b"", True
        finally:
            self.procs.discard(proc)

    def with_suffix(self, prompt):
        return f"{prompt} {self.a.suffix}" if self.a.suffix else prompt

    async def run_once(self, tier, worker, seq, label, token, prompt_id):
        in_flight_at_start = tier.in_flight
        tier.in_flight += 1
        tier.max_in_flight = max(tier.max_in_flight, tier.in_flight)
        t0 = time.time()
        try:
            code, out, err, timed_out = await self.invoke(token, self.with_suffix(self.prompts[prompt_id]))
        finally:
            tier.in_flight -= 1
        t1 = time.time()
        stderr = TOKEN_RE.sub("[token]", err.decode("utf-8", "replace").strip().replace(token, "[token]"))
        session_id, reply, keys = parse_stdout(out)
        error = classify(code, timed_out, self.killing and code is not None and code < 0, stderr, reply)
        if keys and not self.json_keys_shown:
            self.json_keys_shown = True
            log(f"aqua -json output fields: {', '.join(keys)}")
        rec = {
            "tier": tier.users, "worker": worker, "seq": seq, "user": label, "prompt_id": prompt_id,
            "start_utc": utc(t0), "end_utc": utc(t1), "start_ts": round(t0, 3), "end_ts": round(t1, 3),
            "duration_s": round(t1 - t0, 2), "in_flight_at_start": in_flight_at_start,
            "exit_code": code, "timed_out": timed_out, "ok": error is None, "error_class": error,
            "session_id": session_id, "reply_words": len(reply.split()), "stderr_tail": stderr[-600:],
        }
        tier.records.append(rec)
        self.runs_file.write(json.dumps(rec) + "\n")
        if error:
            log(f"[tier {tier.users}] user {worker} run {seq} failed after {t1 - t0:.0f}s: {error}: {stderr[-200:] or '(no stderr)'}")
        if error == "auth":
            self.abort(tier, "authentication error: check the token and --domain", fatal=True)

    # ---- tiers ---------------------------------------------------------------

    def abort(self, tier, reason, fatal=False):
        if tier.abort_reason is None:
            tier.abort_reason = reason
            log(f"[tier {tier.users}] stopping: {reason}")
        self.fatal = self.fatal or fatal
        tier.stop.set()

    async def worker(self, tier, w):
        label, token = self.accounts[w % len(self.accounts)]
        seq = 0
        while not tier.stop.is_set() and (self.a.hold or seq < self.a.runs_per_user):
            await self.run_once(tier, w, seq, label, token, (w + seq) % len(self.prompts))
            seq += 1
            if self.a.think:
                await wait_event(tier.stop, self.a.think)

    def status_line(self, tier, t0):
        recs = tier.records
        bad = sum(1 for r in recs if not r["ok"])
        p50 = pct([r["duration_s"] for r in recs if r["ok"]], 50)
        if tier.stop.is_set():
            phase = "draining"
        elif tier.launched < tier.users:
            phase = "ramping"
        else:
            phase = "holding" if self.a.hold else "running"
        mm, ss = divmod(int(time.time() - t0), 60)
        return (f"[tier {tier.users}] {mm:02d}:{ss:02d} {phase} | users started {tier.launched}/{tier.users} | "
                f"in flight {tier.in_flight} | done {len(recs)} | errors {bad} "
                f"({fmt_pct(bad / len(recs)) if recs else '-'}) | median {fmt_s(p50)}")

    async def monitor(self, tier, t0):
        last = time.time()
        while not tier.finished:
            await asyncio.sleep(1)
            now = time.time()
            recent = [r for r in tier.records if r["end_ts"] >= now - self.a.abort_window]
            if not tier.stop.is_set() and len(recent) >= self.a.abort_min:
                bad = sum(1 for r in recent if not r["ok"])
                if bad / len(recent) >= self.a.abort_error_rate:
                    self.abort(tier, f"rolling error rate {bad / len(recent):.0%} "
                                     f"({bad}/{len(recent)} runs in the last {self.a.abort_window:.0f}s)")
            if now - last >= self.a.status_every:
                last = now
                log(self.status_line(tier, t0))

    async def run_tier(self, users):
        tier = TierState(users)
        self.tier = tier
        t0 = time.time()
        mode = f"hold {self.a.hold:.0f}s" if self.a.hold else f"{self.a.runs_per_user} run(s) each"
        log(f"=== tier {users} users: start one every {self.a.stagger:g}s, {mode} ===")
        mon = asyncio.ensure_future(self.monitor(tier, t0))
        tasks = []
        for w in range(users):
            if tier.stop.is_set():
                break
            tasks.append(asyncio.ensure_future(self.worker(tier, w)))
            tier.launched += 1
            if w < users - 1:
                await wait_event(tier.stop, self.a.stagger)
        ramp_end = time.time()
        if self.a.hold:
            await wait_event(tier.stop, self.a.hold)
            tier.stop.set()
        if tier.in_flight:
            log(f"[tier {users}] all users started; waiting for {tier.in_flight} run(s) to finish")
        await asyncio.gather(*tasks)
        tier.finished = True
        await mon
        return self.summarize(tier, t0, ramp_end, time.time())

    def summarize(self, tier, t0, ramp_end, t_end):
        recs = tier.records
        ok = [r for r in recs if r["ok"]]
        durations = [r["duration_s"] for r in ok]
        errors = collections.Counter(r["error_class"] for r in recs if not r["ok"])
        return {
            "users": tier.users, "runs": len(recs), "ok": len(ok), "errors": len(recs) - len(ok),
            "error_rate": (len(recs) - len(ok)) / len(recs) if recs else None,
            "p50_s": pct(durations, 50), "p95_s": pct(durations, 95), "max_s": max(durations) if durations else None,
            "median_words": pct([r["reply_words"] for r in ok], 50),
            "max_in_flight": tier.max_in_flight, "error_types": dict(errors), "aborted": tier.abort_reason,
            "start_ts": t0, "ramp_end_ts": ramp_end, "end_ts": t_end,
        }

    def gate(self, s, baseline_p50):
        if s["aborted"]:
            return s["aborted"]
        if not s["runs"]:
            return "no runs finished"
        if s["error_rate"] >= self.a.stop_error_rate:
            return f"error rate {s['error_rate']:.1%} reached the {self.a.stop_error_rate:.0%} stop level"
        if self.a.max_slowdown and baseline_p50 and s["p50_s"] and s["p50_s"] > self.a.max_slowdown * baseline_p50:
            return f"median run time {s['p50_s']:.0f}s is over {self.a.max_slowdown:g}x the first tier's {baseline_p50:.0f}s"
        return None

    # ---- reports ---------------------------------------------------------------

    def write_reports(self, stop_reason):
        cols = ["users", "runs", "ok", "errors", "error_rate", "p50_s", "p95_s", "max_s", "slowdown",
                "median_words", "max_in_flight", "error_types", "verdict", "started_utc", "ended_utc"]
        lines = [",".join(cols)]
        for s in self.summaries:
            row = dict(s, error_types=";".join(f"{k}:{v}" for k, v in sorted(s["error_types"].items())),
                       started_utc=utc(s["start_ts"]), ended_utc=utc(s["end_ts"]))
            lines.append(",".join("" if row.get(c) is None else
                                  (f"{row[c]:.4g}" if isinstance(row[c], float) else str(row[c])) for c in cols))
        (self.out / "tiers.csv").write_text("\n".join(lines) + "\n")

        passed = [s["users"] for s in self.summaries if s["verdict"] == "pass"]
        md = [f"# Aqua load test {self.out.name}", "",
              f"- Deployment: {self.a.domain}",
              f"- Accounts: {', '.join(l for l, _ in self.accounts)}",
              f"- Prompt set: `{self.a.prompt}` ({len(self.prompts)} variant(s))",
              f"- A new user every {self.a.stagger:g}s; "
              + (f"each user sends prompts back to back for {self.a.hold:.0f}s" if self.a.hold
                 else f"{self.a.runs_per_user} run(s) per user"),
              f"- The test stops once a tier's error rate reaches {self.a.stop_error_rate:.0%}", "",
              "| Users | Runs | Errors | Error rate | Median | p95 | Slowest | vs first tier | Max in flight | Error types | Result |",
              "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|"]
        for s in self.summaries:
            types = ", ".join(f"{k} {v}" for k, v in sorted(s["error_types"].items())) or "-"
            slow = "-" if s.get("slowdown") is None else f"{s['slowdown']:.2f}x"
            md.append(f"| {s['users']} | {s['runs']} | {s['errors']} | {fmt_pct(s['error_rate'])} | "
                      f"{fmt_s(s['p50_s'])} | {fmt_s(s['p95_s'])} | {fmt_s(s['max_s'])} | {slow} | "
                      f"{s['max_in_flight']} | {types} | {s['verdict']} |")
        md += ["", f"**Highest tier passed:** {max(passed) if passed else 'none'} users.",
               f"**Stopped because:** {stop_reason}." if stop_reason else "**All tiers ran.**", "",
               "Per-run detail, including each run's Aqua session_id, is in `runs.jsonl`."]
        (self.out / "summary.md").write_text("\n".join(md) + "\n")

    # ---- main loop ----------------------------------------------------------------

    def on_signal(self, signum):
        self.interrupts += 1
        if signum == signal.SIGTERM or self.interrupts > 1:
            log("killing in-flight aqua runs (Aqua may still finish them server-side)")
            self.killing = True
            for proc in list(self.procs):
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
        else:
            log("stopping: no new runs; waiting for in-flight runs (Ctrl-C again to kill them)")
        self.stop_all.set()
        if self.tier and not self.tier.finished:
            self.abort(self.tier, "stopped by user")

    async def preflight(self):
        prompt = self.with_suffix(self.a.preflight_prompt)
        for label, token in self.accounts:
            t0 = time.time()
            code, out, err, timed_out = await self.invoke(token, prompt)
            session_id, reply, keys = parse_stdout(out)
            stderr = TOKEN_RE.sub("[token]", err.decode("utf-8", "replace").strip().replace(token, "[token]"))
            error = classify(code, timed_out, False, stderr, reply)
            if error:
                log(f"preflight failed for {label} ({error}): {stderr[-400:] or '(no stderr)'}")
                return False
            log(f"preflight ok for {label} in {time.time() - t0:.0f}s (session {session_id}; "
                f"output fields: {', '.join(keys or ['plain text'])})")
        return True

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.on_signal, sig)
        if self.a.preflight and not await self.preflight():
            return 2
        baseline, stop_reason = None, None
        for i, users in enumerate(self.tiers):
            s = await self.run_tier(users)
            baseline = baseline if baseline is not None else s["p50_s"]
            s["slowdown"] = s["p50_s"] / baseline if baseline and s["p50_s"] else None
            reason = self.gate(s, baseline)
            s["verdict"] = "stop" if reason else "pass"
            self.summaries.append(s)
            log(f"=== tier {users}: {s['runs']} runs, {s['errors']} errors ({fmt_pct(s['error_rate'])}), "
                f"median {fmt_s(s['p50_s'])}, p95 {fmt_s(s['p95_s'])} -> {s['verdict'].upper()}"
                + (f" ({reason})" if reason else ""))
            if self.stop_all.is_set():
                stop_reason = f"stopped by user during the {users}-user tier"
            elif reason:
                stop_reason = f"the {users}-user tier failed the gate: {reason}"
            self.write_reports(stop_reason)
            if stop_reason or i == len(self.tiers) - 1:
                break
            if self.a.pause and input(f"Continue to {self.tiers[i + 1]} users? [y/N] ").strip().lower() not in ("y", "yes"):
                stop_reason = "stopped at the prompt"
                break
            log(f"cooling down {self.a.cooldown:.0f}s before the {self.tiers[i + 1]}-user tier")
            if await wait_event(self.stop_all, self.a.cooldown):
                stop_reason = "stopped by user"
                break
        self.write_reports(stop_reason)
        self.runs_file.close()
        log(f"done. {stop_reason or 'all tiers ran'}. Reports in {self.out}")
        return 2 if self.fatal else 0


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", default=os.environ.get("CODEOCEAN_DOMAIN"), help="deployment URL (default: $CODEOCEAN_DOMAIN)")
    p.add_argument("--tokens", help="file with one '<label> <token>' per line (default: token from the environment)")
    p.add_argument("--token-envs", help="comma-separated env vars that each hold a token (one account each)")
    p.add_argument("--aqua", help="path to the aqua CLI (default: on PATH, else download from the deployment)")
    p.add_argument("--install-dir", default="/tmp/aqua-cli", help="where to download the aqua CLI")
    p.add_argument("--aqua-sha256", help="refuse a downloaded CLI unless its sha256 matches")
    p.add_argument("--no-public-cli", action="store_true", help="only download the CLI from the deployment, never get.codeocean.com")
    p.add_argument("--prompts", default=str(HERE / "prompts.json"), help="JSON file of prompt sets")
    p.add_argument("--prompt", default="long", help="prompt set to use (default: long)")
    p.add_argument("--suffix", default=DEFAULT_SUFFIX, help="text appended to every prompt ('' for none)")
    p.add_argument("--tiers", default="2,4,8", help="comma-separated simulated-user counts (default: 2,4,8)")
    p.add_argument("--max-users", type=int, default=500, help="refuse tiers larger than this, to catch typos (default: 500)")
    p.add_argument("--stagger", type=float, default=1.0, help="seconds between user starts (default: 1)")
    p.add_argument("--runs-per-user", type=int, default=1, help="prompts each user sends (default: 1)")
    p.add_argument("--hold", type=float, default=0, help="instead of --runs-per-user, keep users busy this many seconds")
    p.add_argument("--think", type=float, default=0, help="seconds a user waits between its prompts (default: 0)")
    p.add_argument("--cooldown", type=float, default=60, help="seconds between tiers (default: 60)")
    p.add_argument("--run-timeout", type=float, default=1800, help="kill a run after this many seconds (default: 1800)")
    p.add_argument("--stop-error-rate", type=float, default=0.5, help="stop once a tier's error rate reaches this (default: 0.5)")
    p.add_argument("--abort-error-rate", type=float, help="stop mid-tier once the rolling error rate reaches this (default: --stop-error-rate)")
    p.add_argument("--abort-window", type=float, default=300, help="rolling window in seconds (default: 300)")
    p.add_argument("--abort-min", type=int, default=10, help="runs needed in the window before aborting (default: 10)")
    p.add_argument("--max-slowdown", type=float, default=0, help="also stop if the median run is this many times the first tier's (0 = off)")
    p.add_argument("--no-preflight", dest="preflight", action="store_false",
                   help="skip the short check prompt each account sends before tier 1")
    p.add_argument("--preflight-prompt", default="Reply with the single word: ready. Do not use any tools.")
    p.add_argument("--out", help="output folder (default: runs/<timestamp> next to this script)")
    p.add_argument("--status-every", type=float, default=30, help="seconds between status lines (default: 30)")
    p.add_argument("--pause", action="store_true", help="ask before each new tier")
    p.add_argument("--yes", action="store_true", help="start without asking")
    p.add_argument("--plan", action="store_true", help="print the tier plan and estimates, then exit")
    p.add_argument("--assume-run-s", type=float, default=420, help="run length used for estimates (default: 420)")
    p.add_argument("--assume-tok-s", type=float, default=70, help="output tokens/s used for estimates (default: 70)")
    return p.parse_args()


def main():
    a = parse_args()
    if a.abort_error_rate is None:
        a.abort_error_rate = a.stop_error_rate
    try:
        tiers = [int(t) for t in a.tiers.split(",") if t.strip()]
    except ValueError:
        sys.exit(f"--tiers must be comma-separated whole numbers, got {a.tiers!r}")
    if not tiers or any(t < 1 or t > a.max_users for t in tiers):
        sys.exit(f"each tier must be between 1 and {a.max_users} users (raise --max-users to go higher)")
    if a.plan:
        print_plan(tiers, a)
        return 0
    if not a.domain:
        sys.exit("set --domain or CODEOCEAN_DOMAIN")
    a.domain = a.domain.rstrip("/")
    if not re.match(r"^https?://", a.domain):
        a.domain = "https://" + a.domain
    if a.domain.startswith("http://") and not re.match(r"^http://(localhost|127\.0\.0\.1)(:\d+)?$", a.domain):
        sys.exit("use an https:// deployment URL; plain http is only allowed for localhost")
    accounts = load_accounts(a)
    prompts = load_prompts(a.prompts, a.prompt)
    aqua = ensure_aqua(a)
    raise_fd_limit(max(tiers) * 4 + 256)
    out = Path(a.out) if a.out else HERE / "runs" / datetime.now().strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    print_plan(tiers, a)
    log(f"deployment {a.domain} | accounts: {', '.join(l for l, _ in accounts)} | "
        f"prompt set '{a.prompt}' ({len(prompts)} variant(s)) | aqua CLI {aqua} | output {out}")
    if not a.yes and input("Start? [y/N] ").strip().lower() not in ("y", "yes"):
        return 0
    config = {k: v for k, v in vars(a).items() if k != "tokens"}
    config.update(tiers=tiers, accounts=[l for l, _ in accounts], aqua_cli=aqua)
    (out / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    return asyncio.run(start(a, tiers, accounts, prompts, aqua, out))


async def start(*args):
    # Built inside the running loop: on Python < 3.10, asyncio.Event binds to the loop current at creation.
    return await LoadTest(*args).run()


if __name__ == "__main__":
    sys.exit(main())
