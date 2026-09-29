#!/usr/bin/env python3
"""Stand-in for the aqua CLI, for rehearsing aqua_load_test.py without calling Aqua.

FAKE_MIN_S / FAKE_MAX_S   run length range in seconds (default 1-3)
FAKE_FAIL                 failure probability (default 0)
FAKE_FAIL_ABOVE           fail half the runs while more than this many fakes run at once
FAKE_AUTH_FAIL            if set, every run fails with a 401
"""
import json
import os
import random
import sys
import tempfile
import time
import uuid

args = sys.argv[1:]
if not os.environ.get("CODEOCEAN_TOKEN") or not os.environ.get("CODEOCEAN_DOMAIN"):
    sys.exit("error: CODEOCEAN_TOKEN and CODEOCEAN_DOMAIN must be set")
if os.environ.get("FAKE_AUTH_FAIL"):
    print(f"error: HTTP 401 Unauthorized (token {os.environ['CODEOCEAN_TOKEN']})", file=sys.stderr)
    sys.exit(1)

running_dir = os.path.join(tempfile.gettempdir(), f"fake_aqua_{os.getppid()}")
os.makedirs(running_dir, exist_ok=True)
marker = os.path.join(running_dir, str(os.getpid()))
open(marker, "w").close()
try:
    fail = float(os.environ.get("FAKE_FAIL", "0"))
    above = int(os.environ.get("FAKE_FAIL_ABOVE", "0"))
    if above and len(os.listdir(running_dir)) > above:
        fail = max(fail, 0.5)
    time.sleep(random.uniform(float(os.environ.get("FAKE_MIN_S", "1")), float(os.environ.get("FAKE_MAX_S", "3"))))
    if random.random() < fail:
        print(random.choice([
            "error: invoke failed: HTTP 429 Too Many Requests (ThrottlingException)",
            "error: stream interrupted before the final message",
            "error: 500 Internal Server Error from ConverseStream",
            "error: read timed out",
        ]), file=sys.stderr)
        sys.exit(1)
    reply = "ready" if "single word" in " ".join(args) else "Base editing " * 400
    print(json.dumps({"session_id": str(uuid.uuid4()), "result": reply}) if "-json" in args else reply)
finally:
    os.remove(marker)
