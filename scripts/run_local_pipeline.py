#!/usr/bin/env python3
"""Local standalone orchestrator that replicates camara_ingestion_pipeline's DAG.

Reads the same airflow/dags/config/bundles_config.json the real DAG reads,
rebuilds its (bundle, extractor) dependency graph, and runs each bundle's
bundles/{bundle}/app/runner.py as a local subprocess with a "local" (disk)
destination instead of ECS/S3 -- no AWS involved.
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLES_CONFIG_PATH = REPO_ROOT / "airflow" / "dags" / "config" / "bundles_config.json"

# Each runner subprocess opens its own AsyncCamaraClient with a semaphore of 15
# concurrent HTTP calls, so real concurrency against the external API is
# roughly max_workers * HTTP_SEMAPHORE_PER_WORKER.
HTTP_SEMAPHORE_PER_WORKER = 15

# Runner's own internal handler() caps extraction at 600s (asyncio.wait_for).
# Give a small safety margin so the orchestrator's own timeout never fires
# before the runner's internal one has a chance to raise/print cleanly.
SUBPROCESS_TIMEOUT_S = 650


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"


TERMINAL_OK = {TaskState.SUCCESS, TaskState.PARTIAL}
TERMINAL_BAD = {TaskState.FAILED, TaskState.UPSTREAM_FAILED}

TaskKey = tuple  # (bundle: str, extractor: str)


@dataclass(frozen=True)
class TaskSpec:
    bundle: str
    extractor: str
    depends_on: tuple
    bundle_cfg: dict

    @property
    def key(self):
        return (self.bundle, self.extractor)

    @property
    def task_id(self):
        return f"{self.bundle}_{self.extractor}"

    @property
    def label(self):
        return f"{self.bundle}.{self.extractor}"

    @property
    def log_filename(self):
        return f"{self.bundle}__{self.extractor}.log"


@dataclass
class TaskResult:
    key: TaskKey
    state: TaskState
    attempts: int = 0
    duration_s: float = 0.0
    records: Optional[int] = None
    resumed: bool = False
    note: Optional[str] = None


def load_bundles_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("bundles_config", {})


def build_tasks(bundles_config: dict, bundle_filter: Optional[set]) -> dict:
    tasks = {}
    for bundle_name, cfg in bundles_config.items():
        if bundle_filter and bundle_name not in bundle_filter:
            continue
        for step in cfg.get("sequence", []):
            extractor = step["extractor"]
            tasks[(bundle_name, extractor)] = TaskSpec(
                bundle=bundle_name,
                extractor=extractor,
                depends_on=tuple(step.get("depends_on", [])),
                bundle_cfg=cfg,
            )
    return tasks


def validate_graph(tasks: dict) -> None:
    errors = []

    # Dangling dependency check: a depends_on name must exist as an extractor
    # in the same bundle's sequence.
    for spec in tasks.values():
        for dep in spec.depends_on:
            if (spec.bundle, dep) not in tasks:
                errors.append(
                    f"{spec.label}: depends_on '{dep}' not found in bundle '{spec.bundle}'"
                )

    if errors:
        raise ValueError("Invalid task graph:\n  " + "\n  ".join(errors))

    # Cycle check via Kahn's algorithm, per bundle (deps never cross bundles).
    in_degree = {key: len(spec.depends_on) for key, spec in tasks.items()}
    dependents = {key: [] for key in tasks}
    for spec in tasks.values():
        for dep in spec.depends_on:
            dependents[(spec.bundle, dep)].append(spec.key)

    queue = [k for k, d in in_degree.items() if d == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for nxt in dependents[node]:
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)

    if visited != len(tasks):
        raise ValueError("Invalid task graph: cycle detected in depends_on edges")


def build_event_payload(spec: TaskSpec, run_id: str) -> dict:
    params = {}
    if spec.bundle_cfg.get("init_legislatura") is not None:
        params["init_legislatura"] = spec.bundle_cfg["init_legislatura"]
    return {
        "extractor": spec.extractor,
        "params": params,
        # Deliberately no "path" key: 8/10 runners' local dependency-cache
        # reader reuses the CURRENT task's destination["path"] (if present)
        # instead of the dependency's, so an explicit path would corrupt
        # dependency resolution. Omitting it lets every runner fall back to
        # its own internal (self-consistent) default for both read & write.
        "destination": {"type": "local"},
        "run_id": run_id,
    }


def parse_result_json(text: str) -> Optional[dict]:
    lines = text.splitlines()
    for end_idx in range(len(lines) - 1, -1, -1):
        if lines[end_idx].strip() == "}":
            depth = 0
            for start_idx in range(end_idx, -1, -1):
                stripped = lines[start_idx].strip()
                depth += stripped.count("}") - stripped.count("{")
                if stripped == "{" and depth == 0:
                    candidate = "\n".join(lines[start_idx : end_idx + 1])
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break
            break
    for m in reversed(list(re.finditer(r"\{.*?\}", text, re.DOTALL))):
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            continue
    return None


class PipelineContext:
    def __init__(self, run_id, max_workers, retries, retry_delay, python_bin, log_dir, resume_state):
        self.run_id = run_id
        self.semaphore = asyncio.Semaphore(max_workers)
        self.retries = retries
        self.retry_delay = retry_delay
        self.python_bin = python_bin
        self.log_dir = log_dir
        self.resume_state = resume_state
        self.finished = {}
        self.results = {}
        self.io_lock = asyncio.Lock()
        self.state_path = log_dir / "state.jsonl"


async def log_progress(ctx: PipelineContext, message: str) -> None:
    async with ctx.io_lock:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {message}", flush=True)


async def append_state(ctx: PipelineContext, spec: TaskSpec, result: TaskResult) -> None:
    record = {
        "task_id": spec.task_id,
        "state": result.state.value,
        "records": result.records,
        "attempts": result.attempts,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    async with ctx.io_lock:
        with open(ctx.state_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_resume_state(state_path: Path) -> dict:
    latest = {}
    if not state_path.exists():
        return latest
    for line in state_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        latest[rec["task_id"]] = rec
    return latest


async def execute_once(spec: TaskSpec, attempt: int, ctx: PipelineContext):
    payload = build_event_payload(spec, ctx.run_id)
    env = dict(os.environ)
    env["BUNDLE"] = spec.bundle
    env["EVENT_PAYLOAD"] = json.dumps(payload, ensure_ascii=False)
    env["PYTHONUNBUFFERED"] = "1"
    runner_path = REPO_ROOT / "bundles" / spec.bundle / "app" / "runner.py"
    log_path = ctx.log_dir / spec.log_filename

    start = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        str(ctx.python_bin),
        str(runner_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
        cwd=str(REPO_ROOT),
    )

    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=SUBPROCESS_TIMEOUT_S
        )
        timed_out = False
    except asyncio.TimeoutError:
        proc.kill()
        stdout_bytes, stderr_bytes = await proc.communicate()
        timed_out = True

    duration = time.monotonic() - start
    stdout_text = stdout_bytes.decode("utf-8", errors="replace")
    stderr_text = stderr_bytes.decode("utf-8", errors="replace")

    with open(log_path, "a", encoding="utf-8") as f:
        if attempt > 1:
            f.write(f"\n=== attempt {attempt} ===\n")
        f.write("--- stdout ---\n")
        f.write(stdout_text)
        f.write("\n--- stderr ---\n")
        f.write(stderr_text)
        f.write("\n")

    if timed_out:
        return TaskState.FAILED, None, "orchestrator-level timeout exceeded", duration

    if proc.returncode != 0:
        tail = "\n".join(stderr_text.splitlines()[-20:]) or "(no stderr output)"
        return TaskState.FAILED, None, tail, duration

    parsed = parse_result_json(stdout_text)
    if parsed is None:
        return TaskState.FAILED, None, "no parseable JSON result in stdout despite exit 0", duration

    status = parsed.get("status")
    state = TaskState.PARTIAL if status == "partial" else TaskState.SUCCESS
    return state, parsed.get("records"), None, duration


async def run_task(spec: TaskSpec, tasks: dict, ctx: PipelineContext) -> None:
    for dep_extractor in spec.depends_on:
        await ctx.finished[(spec.bundle, dep_extractor)].wait()

    dep_states = [ctx.results[(spec.bundle, d)].state for d in spec.depends_on]
    if any(s in TERMINAL_BAD for s in dep_states):
        blockers = [
            f"{spec.bundle}.{d}"
            for d in spec.depends_on
            if ctx.results[(spec.bundle, d)].state in TERMINAL_BAD
        ]
        result = TaskResult(spec.key, TaskState.UPSTREAM_FAILED, note=f"blocked by {', '.join(blockers)}")
        ctx.results[spec.key] = result
        await append_state(ctx, spec, result)
        await log_progress(ctx, f"SKIP     UPSTREAM_FAILED  {spec.label:30s} ({result.note})")
        ctx.finished[spec.key].set()
        return

    prior = ctx.resume_state.get(spec.task_id)
    if prior and prior.get("state") in ("SUCCESS", "PARTIAL"):
        result = TaskResult(
            spec.key,
            TaskState(prior["state"]),
            records=prior.get("records"),
            resumed=True,
        )
        ctx.results[spec.key] = result
        await log_progress(ctx, f"RESUME   {result.state.value:16s} {spec.label:30s} (from prior run)")
        ctx.finished[spec.key].set()
        return

    attempts_allowed = 1 + ctx.retries
    state = TaskState.FAILED
    records = None
    note = None
    duration = 0.0
    attempt = 0
    for attempt in range(1, attempts_allowed + 1):
        async with ctx.semaphore:
            await log_progress(ctx, f"START             {spec.label:30s} (attempt {attempt}/{attempts_allowed})")
            state, records, note, duration = await execute_once(spec, attempt, ctx)
        if state in TERMINAL_OK or attempt == attempts_allowed:
            break
        await log_progress(ctx, f"RETRY             {spec.label:30s} -> {ctx.log_dir / spec.log_filename}")
        await asyncio.sleep(ctx.retry_delay)

    result = TaskResult(spec.key, state, attempts=attempt, duration_s=duration, records=records, note=note)
    ctx.results[spec.key] = result
    await append_state(ctx, spec, result)

    extra = f" ({result.records} records)" if result.records is not None else ""
    if result.note and result.state in TERMINAL_BAD:
        extra += f" -> {ctx.log_dir / spec.log_filename}"
    await log_progress(
        ctx, f"{result.state.value:8s} {result.duration_s:6.1f}s  {spec.label:30s}{extra}"
    )
    ctx.finished[spec.key].set()


async def orchestrate(tasks: dict, ctx: PipelineContext) -> dict:
    for key in tasks:
        ctx.finished[key] = asyncio.Event()
    await asyncio.gather(*(run_task(spec, tasks, ctx) for spec in tasks.values()))
    return ctx.results


def render_dry_run(tasks: dict) -> str:
    by_bundle = {}
    for spec in tasks.values():
        by_bundle.setdefault(spec.bundle, []).append(spec)
    lines = [f"Resolved task graph: {len(tasks)} tasks across {len(by_bundle)} bundles\n"]
    for bundle in sorted(by_bundle):
        lines.append(f"{bundle}:")
        for spec in by_bundle[bundle]:
            deps = ", ".join(spec.depends_on) if spec.depends_on else "-"
            lines.append(f"  {spec.extractor:24s} depends_on=[{deps}]")
    return "\n".join(lines)


def render_summary_table(tasks: dict, results: dict) -> str:
    header = f"{'BUNDLE':12s} {'EXTRACTOR':22s} {'STATUS':17s} {'DURATION':>10s} {'RECORDS':>9s}  NOTES"
    sep = "-" * 12 + " " + "-" * 22 + " " + "-" * 17 + " " + "-" * 10 + " " + "-" * 9 + "  " + "-" * 28
    rows = [header, sep]
    counts = {s: 0 for s in TaskState if s not in (TaskState.PENDING, TaskState.RUNNING)}
    for key in sorted(tasks, key=lambda k: (k[0], k[1])):
        spec = tasks[key]
        result = results.get(key)
        if result is None:
            continue
        counts[result.state] = counts.get(result.state, 0) + 1
        duration = f"{result.duration_s:.1f}s" if result.duration_s else "-"
        records = str(result.records) if result.records is not None else "-"
        note = result.note or ("resumed" if result.resumed else "")
        rows.append(
            f"{spec.bundle:12s} {spec.extractor:22s} {result.state.value:17s} {duration:>10s} {records:>9s}  {note}"
        )
    rows.append(sep)
    totals = ", ".join(f"{count} {state.value}" for state, count in counts.items() if count)
    rows.append(f"Totals: {totals}")
    return "\n".join(rows)


def resolve_python_bin(explicit: Optional[str]) -> Path:
    if explicit:
        return Path(explicit)
    venv_python = REPO_ROOT / "venv" / "bin" / "python"
    if venv_python.exists():
        return venv_python
    print(f"[WARNING] {venv_python} not found; falling back to {sys.executable}", file=sys.stderr)
    return Path(sys.executable)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the camara_ingestion_pipeline DAG's routine locally, without AWS/Airflow.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bundle", default=None, help="Comma-separated bundle names to restrict to (default: all)")
    parser.add_argument("--run-id", default=None, help="Reuse an existing run_id to resume/skip succeeded tasks")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help=f"Max concurrent runner subprocesses. Each worker opens up to "
        f"{HTTP_SEMAPHORE_PER_WORKER} concurrent HTTP calls, so the real ceiling "
        f"against the external API is roughly max-workers * {HTTP_SEMAPHORE_PER_WORKER}.",
    )
    parser.add_argument("--retries", type=int, default=1, help="Retry attempts per task after first failure")
    parser.add_argument("--retry-delay", type=float, default=5.0, help="Delay in seconds before a retry")
    parser.add_argument("--config", default=str(BUNDLES_CONFIG_PATH), help="Path to bundles_config.json")
    parser.add_argument("--python", default=None, help="Python interpreter to run runners with (default: venv/bin/python)")
    parser.add_argument("--fresh", action="store_true", help="Ignore any existing state.jsonl for --run-id")
    parser.add_argument("--dry-run", action="store_true", help="Print the resolved task graph and exit")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    bundles_config = load_bundles_config(Path(args.config))
    bundle_filter = set(args.bundle.split(",")) if args.bundle else None
    tasks = build_tasks(bundles_config, bundle_filter)
    if not tasks:
        print("[ERROR] No tasks resolved -- check --bundle/--config", file=sys.stderr)
        return 1
    validate_graph(tasks)

    if args.dry_run:
        print(render_dry_run(tasks))
        return 0

    run_id = args.run_id or f"local-{int(time.time())}"
    log_dir = REPO_ROOT / "local_run_logs" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)

    resume_state = {} if args.fresh else load_resume_state(log_dir / "state.jsonl")
    if resume_state:
        done = sum(1 for r in resume_state.values() if r.get("state") in ("SUCCESS", "PARTIAL"))
        print(f"[INFO] Resuming run_id={run_id}: {done} task(s) already recorded as SUCCESS/PARTIAL.")
        print("[INFO] Note: resume only skips re-running tasks; it does not guarantee their")
        print("[INFO] /tmp/{bundle}/... cache files still exist -- a cache-miss falls back to a")
        print("[INFO] safe (slower) live recomputation inside the runner itself.")

    python_bin = resolve_python_bin(args.python)
    print(f"[INFO] run_id={run_id}  tasks={len(tasks)}  max_workers={args.max_workers}  python={python_bin}")

    ctx = PipelineContext(
        run_id=run_id,
        max_workers=args.max_workers,
        retries=args.retries,
        retry_delay=args.retry_delay,
        python_bin=python_bin,
        log_dir=log_dir,
        resume_state=resume_state,
    )

    results = asyncio.run(orchestrate(tasks, ctx))

    summary = render_summary_table(tasks, results)
    print("\n" + summary)
    (log_dir / "summary.txt").write_text(summary, encoding="utf-8")
    print(f"\nlogs: {log_dir}")

    bad = [r for r in results.values() if r.state in TERMINAL_BAD]
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
