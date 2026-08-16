# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

The project uses `uv`. Do not use `poetry` or a bare `python`/`pytest` — always go through `uv run`.

```bash
uv sync --all-groups --all-extras   # install dev environment
make test                           # format + lint + unittest (the usual pre-push check)
make format                         # ruff format
make lint                           # ruff check + mypy (both src AND tests)
make unittest                       # uv run pytest
make coverage                       # pytest with coverage report
uv run tox                          # full matrix: py310/311/312 + format + lint + build
uv run pytest tests/test_logic_tree.py::test_registered_digests_unchanged   # a single test
```

Ruff config: line length 120, `quote-style = "preserve"` (do not normalise quotes), lint rules `E,F,I,B,UP,G004`. `G004` means no f-strings in logging calls — use `log.info("... %s", value)`. mypy runs over `tests/` too, so test helpers need to typecheck.

Version is derived from the git tag by hatch-vcs; `toshi_hazard_post/_version.py` is generated, never edit it.

## Running the app

```bash
thp aggregate demo/hazard_mini.toml     # or: uv run thp aggregate ...
```

Runtime config comes from env vars, or a `.env` file (override the path with `THP_ENV_FILE`): `THP_RLZ_DIR` and `THP_AGG_DIR` (local path or `s3://` URI), `THP_NUM_WORKERS`, `THP_WORKING_DIR`. The calculation itself is described by a TOML input file — see `docs/usage.md` and the `demo/` directory.

**Gotcha:** `local_config.py` reads the environment once at import time into module-level constants, and consumers do `from ... import AGG_DIR`, binding the value at *their* import time. Setting an env var after import has no effect. Tests monkeypatch the consuming module's attribute instead (`monkeypatch.setattr(toshi_hazard_post.data, 'AGG_DIR', ...)`).

## Architecture

A hazard curve is aggregated from pre-computed *realizations* held in an external pyarrow/parquet dataset (written by `toshi-hazard-store`). This app never computes hazard from source models — it selects, recombines, and aggregates rows from that dataset.

### Hash digests are the join key

This is the central idea and it spans `logic_tree.py`, `data.py`, and `aggregation_calc.py`. Every branch has a `registry_identity` string; its `identity_digest` is the primary key used to find that branch's realization rows in the dataset. `HazardComponentBranch.hash_digest` is `sources_digest + gmms_digest` concatenated, and the same concatenation is rebuilt from dataset columns in `create_component_dict()`. If digests don't line up, nothing matches.

Digests are *computed*, not looked up: `nzshm_model`'s branch registry is advisory only, so an unregistered branch still gets a valid digest and only warns (`_branch_hash_digest` in `logic_tree.py`, memoised). The practical consequence is that a mistyped toshi id no longer fails fast — it produces a well-formed digest that matches nothing, and the run dies much later in `get_job_datatable` with `incorrect number of records found`. That warning is the clue.

### Component vs composite branches

The distinction drives the whole data model:

- **Component branch** (`HazardComponentBranch`) — one SRM branch plus its TRT-matched GMCM branches. This is the smallest unit with a stored realization, and there are relatively few of them. Used to *query* the dataset.
- **Composite branch** (`HazardCompositeBranch`) — one full realization of the entire logic tree, formed by the cartesian product across branch sets. There are millions for a production model. Used to *aggregate*.

Rates are additive, probabilities are not — hence the prob→rate→aggregate→prob round trip. A composite branch's rate is the plain sum of its component branches' rates (`calc_composite_rates`).

`HazardLogicTree` drops GMCM branch sets whose TRT is absent from the source tree, then produces two big arrays: `weights` (one per composite branch) and `branch_hash_table` (each composite branch as a list of component digests).

### Execution flow

`cli.py` → `aggregation.run_aggregation()` is the orchestrator:

1. Resolve sites and logic trees (`aggregation_setup.py`), build `HazardLogicTree`.
2. Compute `weights` and `branch_hash_table`, copy both into **named shared memory** so worker processes read them without pickling (names in `constants.py`).
3. `_generate_agg_jobs()` batches work by `(vs30, nloc_0)` — a 1° location bin — so the parquet dataset is scanned once per batch rather than per site. Each batch is sliced per `(location, imt)` and written as an ORC file to `WORKING_DIR`.
4. Each ORC file becomes one task on a `ProcessPoolExecutor`; `aggregation_calc.calc_aggregation()` loads it, converts to rates, sums into composite rates via the shared hash table, computes weighted aggregates, converts back to probabilities, saves, and deletes its ORC file.

Because the shared memory segments have fixed names and are created with `create=True` and unlinked without a `try/finally`, a crashed run leaves them behind and the next run fails with `FileExistsError`. Two concurrent runs on one machine collide for the same reason.

`run_aggregation` accepts an `Executor` argument — `test_end_to_end.py` passes a `ThreadPoolExecutor` because monkeypatching does not survive `spawn`-based multiprocessing on macOS/Windows.

### Module map

- `aggregation.py` — orchestration, batching, shared memory, process pool
- `aggregation_calc.py` — the per-task calculation
- `aggregation_args.py` — pydantic models for the TOML input, with cross-field validation and path resolution relative to the config file
- `aggregation_setup.py` — resolving sites and logic trees
- `logic_tree.py` — the combined SRM+GMCM tree and branch digests
- `data.py` — all pyarrow dataset reads/writes
- `calculators.py` — numba `@jit` numerical kernels (mean/std/cov/quantiles, prob↔rate)

## Releasing

Add a `CHANGELOG.md` entry, then tag: `git tag vX.Y.Z && git push && git push --tags`. GitHub Actions publishes to PyPI and deploys docs if tests pass.
