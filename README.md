# liquidcomp

Run [throng](https://github.com/mutating/throng) commands on your Liquid Compute fleet. Personal and organization tokens use the same API; the server determines which computers are available to each token.

Requires Python 3.8+ and throng 0.0.10+. The remote platform is fixed to `linux / arm64 / powercell-ubuntu-arm64-v1`.

## Installation and usage

```sh
pip install liquidcomp
liquid init
liquid status
```

Installing the package automatically registers the `liquid` throng plugin:

```python
from throng import throng

manager = throng['liquid'].one('./project')
result = manager.run('python3 task.py')
print(result.stdout)

with manager.scope as isolate:
    isolate.run('python3 first.py', exception=True)
    results = isolate.chain('python3 second.py', 'python3 third.py')
    snapshot = isolate.read()  # tar bytes containing the current remote working tree
```

Each manager allocation uploads an independent tar snapshot of the source directory. Commands in the same isolate share its working directory; local files are not changed. `exclude` uses gitignore-style patterns. Empty directories, file permissions, and internal symlinks are preserved. Use a context manager to release remote capacity promptly.

The normal throng arguments `path`, `exclude`, `prepare`, and `packages` are supported. Preparation and package installation run before returning the isolate. `packages` installs Python dependencies with `python3 -m pip`; failure triggers throng's usual cleanup and exceptions.

For explicit settings:

```python
from cantok import TimeoutToken
from liquidcomp import LiquidManager, Settings

manager = LiquidManager(
    './project',
    exclude=['.git/', 'venv/', '*.secret'],
    settings=Settings(cpu=2, ram=1, disk=2, command_timeout=7200),
)
with manager.scope as isolate:
    result = isolate.run('python3 task.py', token=TimeoutToken(30))
```

`LiquidResult` extends throng's result with `id`, `status`, `error`, and `killed_by_token`. A known command failure returns an unsuccessful result unless `exception=True` (or another throng exception policy) is supplied. Infrastructure, authentication, and protocol failures raise exceptions. Full log artifacts replace inline previews; binary artifacts are also available as `stdout_bytes` / `stderr_bytes`, with UTF-8 replacement decoding in the corresponding text fields.

## Credentials

Credentials are resolved and checked through `GET /v1/auth/check` when an isolate is created. Constructing a manager and reading local state do not access the network or keyring.

Resolution order:

1. `LIQUID_TOKEN` from the process environment, including an explicitly empty value.
2. The keyring secret with username `LIQUID_TOKEN` and service `liquidcomp:<api_url without trailing slashes>`.
3. If absent, `MissingTokenError` by default. With `ask_for_token=true`, allocation calls the manager's `init()` method to open the shared dialog.

An empty, malformed, expired, revoked, or insufficiently privileged environment token causes an error. It never falls back to keyring or prompts. A saved but rejected token also causes an error; explicit `liquid init` replaces it. Token text is never read from configuration files or passed as a CLI argument.

Both `read` and `execute` permissions are required. The plugin treats the token as opaque and does not select a personal or organization authentication mode. A successful check confirms authorization, not available fleet capacity.

Keyring uses the installed operating-system backend. Backend read/write failures raise `KeyringError`; they do not count as a missing secret or activate a plaintext fallback. Saved credentials are scoped to the API URL, so configuring another server requires its own credential.

## CLI

### `liquid init`

Always offers to enter a new token, including when a token already exists. The existing source is reported without revealing its value; Ctrl+C or EOF cancels with exit status **130** and preserves the saved secret. The same dialog is available as `LiquidManager.init()`.

Input is hidden and requires a terminal. Empty or whitespace-only input retries. A token rejected by the server (401/403) retries. A token is saved to keyring, and the user is thanked, only after the API confirms `read` and `execute`. Network or protocol failures leave the previous secret unchanged. A keyring write failure is reported without printing success. Echoed fallback input is disabled; piped stdin is rejected.

If `LIQUID_TOKEN` is present, initialization explains that it retains priority. The newly saved keyring token takes effect after that variable is removed.

### `liquid status`

Checks server connectivity/readiness and token availability/validity without prompting or creating an isolate. Prints the credential source, never its value. Uses the origin's unauthenticated `/health/ready` endpoint and the configured `/v1/auth/check` endpoint.

Exit status is **0** only when the server is ready and the token is accepted; **1** otherwise. A failed network request, unavailable endpoint, malformed reply, or server error means token validity is **unknown**. A 401/403 means the token is invalid or lacks permissions. A health response can prove connectivity while reporting that the server is not ready. Invalid CLI usage exits **2**.

The corresponding programmatic method `manager.status()` returns a `Status` value with `server_connected`, `server_ready`, `token_available`, `token_valid` (`True`, `False`, or `None`), `token_source`, and `error`.

## Configuration

All ordinary settings are read through [skelet](https://github.com/mutating/skelet). Each new settings object reads fresh sources. Explicit `Settings(...)` arguments override loaded values. The environment takes precedence over files; file lookup follows skelet's `for_tool('liquid')` order: `liquid.toml`, `.liquid.toml`, `[tool.liquid]` in `pyproject.toml`, then the equivalent YAML and JSON sources in the current working directory.

For example, in `pyproject.toml`:

```toml
[tool.liquid]
cpu = 2
ram = 1
disk = 2
command_timeout = 7200
ask_for_token = false
```

Or use environment variables such as `LIQUID_CPU=2`, `LIQUID_COMMAND_TIMEOUT=7200`, and `LIQUID_ASK_FOR_TOKEN=true`. Dictionary values in the environment use JSON, for example `LIQUID_ENV='{"LANG":"C.UTF-8"}'`.

| Setting | Environment variable | Default | Meaning / allowed values |
| --- | --- | --- | --- |
| `api_url` | `LIQUID_API_URL` | `https://api.mutating.tech/v1` | API base URL; HTTP(S), no credentials, query, or fragment. Include `/v1`. |
| `ask_for_token` | `LIQUID_ASK_FOR_TOKEN` | `false` | Open `manager.init()` when no credential exists. |
| `image` | `LIQUID_IMAGE` | `ubuntu` | Image supported by the fixed runtime; the server validates supported references. |
| `cpu` | `LIQUID_CPU` | `1` | vCPUs, converted to integer millicores; at least 1 millicore, at most 1000 vCPUs. |
| `ram` | `LIQUID_RAM` | `0.5` | GiB, converted to integer bytes; 1 byte through 1 EiB. |
| `disk` | `LIQUID_DISK` | `1` | GiB, converted to integer bytes; 1 byte through 1 EiB. |
| `command_timeout` | `LIQUID_COMMAND_TIMEOUT` | `3600` | Integer seconds, 1–86400 (24 hours); sent for every command. |
| `shell` | `LIQUID_SHELL` | `false` | Enable the remote Linux shell; otherwise commands use argument parsing. |
| `cwd` | `LIQUID_CWD` | `.` | Command directory relative to the uploaded tree; at most 1024 characters, no traversal. |
| `env` | `LIQUID_ENV` | `{}` | Command environment overrides; at most 100 string pairs, names ≤200 characters and values ≤8192. |
| `request_timeout` | `LIQUID_REQUEST_TIMEOUT` | `10` | HTTP connect/read/write/pool timeout and per-attempt response read budget, seconds. |
| `poll_interval` | `LIQUID_POLL_INTERVAL` | `0.2` | Seconds between result/control polls and cancellation-token checks. |
| `operation_timeout` | `LIQUID_OPERATION_TIMEOUT` | `300` | Wait budget for provisioning or snapshot completion, seconds. |
| `cancellation_timeout` | `LIQUID_CANCELLATION_TIMEOUT` | `40` | Wait budget for a terminal result after cancellation is requested, seconds. |
| `renew_interval` | `LIQUID_RENEW_INTERVAL` | `60` | Maximum interval between lease renewals, seconds; shortened according to the server's lease. |
| `retry_attempts` | `LIQUID_RETRY_ATTEMPTS` | `3` | Total attempts per retryable request, 1–10. |
| `retry_backoff` | `LIQUID_RETRY_BACKOFF` | `0.1` | Initial exponential retry delay, 0–10 seconds, capped by `request_timeout`. |
| `max_artifact_bytes` | `LIQUID_MAX_ARTIFACT_BYTES` | `104857600` | Positive limit for state uploads and artifact downloads, including tar overhead. |
| `max_response_bytes` | `LIQUID_MAX_RESPONSE_BYTES` | `16777216` | Positive limit for a JSON/control response. |

All timing settings other than the separately constrained command timeout and retry backoff accept finite positive values up to 86400 seconds. CPU, RAM, disk, and timing values accept integers and fractions. Polling pauses respect provisioning, snapshot, and cancellation budgets even when `poll_interval` is longer. HTTP attempts and retries can extend the time before a control timeout is reported; configure the transport budget alongside control budgets. Configuration is validated on construction/assignment. Avoid mutating settings during active operations.

OS, architecture, and runtime have no settings and cannot be overridden. Keyring service naming and backend selection are separate from ordinary settings; standard keyring configuration selects the backend. `LIQUID_TOKEN` is read through a dedicated skelet environment source and is never included in ordinary `Settings`.

## Benchmark metadata

```python
with manager.scope as isolate:
    meta = isolate.meta
    print(meta.id, meta.resources.shared)  # stable namespaced ID, True
    single = meta.resources.benchmarks.single_core
    if single is not None:
        print(single.number, single.when, single.benchmarker_version)
```

The plugin requests `/v1/computers/{computer_id}/benchmark-results?latest_only=true` for the allocated computer. It maps `single_core` and `multi_core` to throng's benchset results. Scores describe the whole computer and are not rescaled to the isolate's resource reservation. `shared=True`; `shared_with=None` because the API does not report the current sharing count.

An empty suite or an absent individual measurement becomes `None`. Only the latest suite is used; an older result is never substituted for a missing newer measurement. Unknown future benchmark names are ignored. Each access fetches fresh metadata. `when` is the API's timezone-aware `created_at`, normalized to UTC. Currently the server preserves `created_at` when upserting the same benchmark/version, so it may describe the first recorded measurement rather than the most recent rerun.

Permission, network, HTTP, and malformed-data failures raise `MetadataError`, rather than pretending that benchmarks are absent. A metadata failure does not disable subsequent commands. Scores, versions, dates, resource identity, duplicate records, and suite consistency are validated. A closed or failed isolate still raises its lifecycle error.

## Edge cases and failure behavior

- Credentials are checked before uploading state or reserving resources. Missing credentials raise `MissingTokenError`, a subclass of `ThrongError` through `LiquidError`. Other plugin errors are also derived from `LiquidError`; HTTP errors include sanitized status information without response bodies or credentials.
- An explicit environment token has unconditional priority, even if empty or invalid. Initialization never overwrites the environment. A locked/unavailable keyring is an error, not an empty vault.
- Token validation can succeed just before revocation or a permissions change. Later API denials are propagated; no automatic credential replacement occurs. The server controls the token's fleet scope.
- Isolates renew their leases in one background worker throughout provisioning and execution, including while idle or downloading/polling. Renewal deadlines are independent of result polling. A lost or expired lease fails waiting operations. Automatic execution retry on another computer is disabled (`retry_allowed=false`).
- Cancellation before allocation launches nothing. Cancellation during allocation attempts to release known resources. Cancellation before a command returns an unsuccessful result without submitting it. After submission, cancellation is requested through the API and the plugin waits for an actual terminal result: HTTP 202 alone is not completion. A successful completion can win a cancellation race. Unconfirmed cancellation raises `CancellationTimeoutError`.
- Cancellation monitoring ends when a terminal result arrives, before log artifacts are downloaded. A stale cancellation response cannot fail the isolate after that command has finished; errors canceling a still-active command remain fatal.
- `killed_by_token` records whether the token initiated cancellation and the API confirmed a canceled/timed-out outcome. A later token change, including during log retrieval, cannot turn a watchdog timeout into token cancellation.
- Once the API confirms a terminal result for the expected command, decoding or log-download errors fail that call without disabling the isolate or canceling the completed command. In particular, corrupt checksums still raise `ProtocolError`; subsequent commands can proceed. Uncertain execution and mismatched command identities remain fatal.
- The server enforces the command timeout. A local watchdog also bounds a missing terminal outcome using the API queue deadline plus the command timeout, followed by the cancellation budget. A stopped client cannot forcibly terminate a disconnected executor; server-side lease and timeout enforcement remain necessary.
- Retryable connection failures and HTTP 408/425/429/5xx use bounded retries. Isolate creation, command submission, and snapshots retain the same idempotency key across retries. Commands are never silently resubmitted with a new key. An uncertain API error exposes `idempotency_key` where available; the operation may already have happened.
- An uncertain command submission or a malformed acceptance response disables further execution, snapshots, and metadata requests on that isolate. A known command ID is canceled on a best-effort basis; create a new isolate rather than continuing an uncertain sequence.
- Artifact uploads are not retried after an uncertain response because the current API does not support upload idempotency. Unknown remote IDs cannot be cleaned up by the client; server retention/expiry applies. Known failed allocations are cleaned up on a best-effort basis, preserving the original error.
- `kill()` is locally idempotent, stops the renewal worker, attempts remote destruction, and closes the HTTP client. A destruction error is reported; the client does not keep renewing an isolate it failed to destroy. Server lease expiry provides eventual cleanup. Destructors suppress cleanup errors; explicit context management exposes them.
- Commands on a single isolate are serialized. A manager can allocate several independent isolates concurrently. Closing waits for in-flight operations before closing the HTTP client.
- Initial state rejects missing/unreadable directories, dangling or escaping symlinks, and special files such as FIFOs. Links inside the source tree are rewritten to relative targets. Excluded files/directories are not uploaded; exclusions do not read `.gitignore` automatically. Size limits include tar headers and padding.
- Artifact downloads must match `X-Content-SHA256`. Oversized, malformed, mismatched, or contradictory API replies raise `ProtocolError`. Redirects are not followed, and HTTP proxy environment variables are not inherited by the transport.
- The bundled ARM64 runtime and its images must contain the executables used by your commands. `packages` requires working Python/pip and the corresponding remote network access. Runtime image availability, resources, and shared isolation policies are decided by the server.

## Development and tests

```sh
uv venv --python 3.12 venv
uv pip install --python venv/bin/python -r requirements_dev.txt -e .
venv/bin/python -m pytest tests
venv/bin/ruff check liquidcomp tests
venv/bin/mypy --strict liquidcomp
```

Tests are written before implementation. Unit tests cover credential priority, every dialog outcome, configuration, protocol boundaries, cancellation, lease maintenance, state transfer, metadata, and cleanup. E2E tests use an independent local HTTP server, real subprocess commands, and a test-only keyring. Unix terminal tests verify hidden input and cancellation with a real pseudo-terminal. They are skipped where PTYs are unavailable. Tests never contact a production API or access the operating-system vault.

The existing CI matrix runs tests across Linux, macOS, and Windows with Python 3.8–3.15, including free-threaded Python, and requires 100% statement and branch coverage. Its subprocess coverage startup is defined in [.github/workflows/tests_and_coverage.yml](.github/workflows/tests_and_coverage.yml).
