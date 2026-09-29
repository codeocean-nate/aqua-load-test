# Aqua load test

Measures how Aqua, Code Ocean's AI assistant, and the model backend behind it hold up as more people use it at once. It runs from inside a Code Ocean capsule and drives Aqua through the [aqua CLI](https://get.codeocean.com/aqua-cli/), in tiers of simulated users. For example, 2, then 4, then 8 users each send a prompt at about the same time. After each tier it checks the error rate, and it moves up until a tier reaches the stop level (50% by default).

> **Before you run it:** every simulated user is a real Aqua conversation billed to your model backend. The large tiers can use millions of tokens and compete with real users for quota. Agree on a time window with whoever owns the backend's quota, and start small.

## Files

| File | What it is |
|---|---|
| `run` | Capsule entry point (see parameters below). |
| `aqua_load_test.py` | The harness: Python 3.8+ standard library, no packages to install. |
| `prompts.json` | Prompt sets. `long` asks for a ~15,000-word review with no tools (about 5–10 min per run); `short` takes about 1 min. |
| `dev/fake_aqua.py` | A stand-in CLI for rehearsing without calling Aqua. |

## Set up a capsule

1. Create a capsule with a Python starter environment and put these files in `/code`, for example by cloning this repository.
2. In **Account > Roles and Secrets**, add a custom-key secret that holds a Code Ocean API token. Then, in the capsule, go to **Settings > Credentials**, add the secret, and commit. The harness uses `$CODEOCEAN_TOKEN` if it is set, otherwise the first environment variable whose value starts with `cop_`. It never prints the token.
3. Set `DEFAULT_DOMAIN` in `run` to your deployment's URL, or pass the URL as parameter 6.
4. Optional: install the CLI in the environment's postInstall script:
   ```bash
   curl -fsSL "https://<your-deployment>/downloads/aqua-cli/aqua.linux.amd64.tar.gz" | tar -xz -C /usr/local/bin aqua
   ```
   Without this step, the harness downloads the CLI at run time. It tries your deployment first, then get.codeocean.com (`--no-public-cli` turns off the second), always over HTTPS, and logs the binary's sha256. Pass `--aqua-sha256` to require a specific build.
5. Choose a machine size: each simulated user is one CLI process, so allow about 16 GB of memory for tiers of a few hundred.

## Parameters (`run`, in App Panel order)

| # | Parameter | Default |
|---|---|---|
| 1 | Tiers: simulated-user counts | `2,4,8` |
| 2 | Prompt set | `long` |
| 3 | Runs per user | `1` |
| 4 | Seconds between user starts | `1` |
| 5 | Stop once a tier's error rate reaches | `0.5` |
| 6 | Deployment URL | `DEFAULT_DOMAIN` |

Run `python3 aqua_load_test.py --help` for the rest: `--hold` keeps users busy for a set time instead of a set number of runs, and `--think` adds a pause between a user's prompts. Tiers above 500 users need `--max-users`.

Before tier 1, a short check prompt runs once per account. A bad token or URL therefore stops the test right away, and a mid-tier 401 or 403 stops it too.

## Outputs (`/results`)

- `summary.md`: per-tier runs, errors, median and p95 run time, the slowdown compared with the first tier, peak concurrency, error types, and why the test stopped.
- `tiers.csv`: the same table as CSV.
- `runs.jsonl`: one line per run, with timing, exit code, error class, the Aqua `session_id` and the last lines of stderr (tokens redacted).
- `config.json`: the settings used, without tokens.

## Error classes

The CLI's stderr is sorted into these classes: `auth`, `throttle` (429), `session_busy` (409), `upstream_5xx`, `timeout`, `stream` (dropped connection), `form` (Aqua asked a question), `empty_reply`, `client_timeout` (killed after `--run-timeout`), `killed` and `other`.

## Rehearse locally

```bash
CODEOCEAN_TOKEN=cop_fake python3 aqua_load_test.py --aqua dev/fake_aqua.py --domain example.com --cooldown 1 --yes
```

The fake CLI's `FAKE_FAIL` and `FAKE_FAIL_ABOVE` settings simulate failures (see the top of `dev/fake_aqua.py`).
