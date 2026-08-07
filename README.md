# mecon_lib

Domain library for the Mecon ecosystem (transactions, tags, ETL transformers).
Consumed by `mecon_app` and `mecon_etl_dag`.

## Development

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

### Setup

```bash
uv sync --group dev
```

### Test

```bash
make test
```

### Lint

```bash
uv run ruff check .
```

## Releasing

This package is distributed as a **git tag** — there is no PyPI or artifact
registry. Consumers depend on it as a VCS dependency:

```toml
dependencies = [
    "mecon @ git+https://github.com/minime-lab/mecon_lib.git@v1",
]
```

The `v1` tag is a **floating major tag**: it always points at the newest `1.x`
release. Pinning `@v1` means consumers get every backwards-compatible update but
never an unannounced `2.0.0`.

### Cut a new version

From a clean `main`:

```bash
make release VERSION=1.2.6
```

That single command:

1. runs the tests (the release aborts if they fail)
2. bumps `version` in `pyproject.toml` and refreshes `uv.lock`
3. commits the bump
4. tags the release `1.2.6` (immutable) and moves `v1` onto the same commit
5. pushes `main`, the new tag, and the force-updated `v1`

`VERSION` is required and must be `X.Y.Z` — pick it yourself following semver:

| Change | Bump | Example |
| --- | --- | --- |
| Bug fix, internal refactor | patch | `1.2.5` → `1.2.6` |
| New backwards-compatible feature | minor | `1.2.5` → `1.3.0` |
| Breaking change | major | `1.2.5` → `2.0.0` |

A major bump rolls **`v2`** instead and leaves `v1` frozen, so existing
consumers keep resolving to the last `1.x` until you migrate them deliberately.

Note that release tags here are bare (`1.2.6`), while `minime_utils` prefixes
them (`v1.0.4`); the floating tag is `v1` in both. This is set by `TAG_PREFIX`
at the top of the `Makefile`.

The release refuses to run unless you are on `main`, the tree is clean (ignoring
untracked files), `main` is up to date with `origin`, and the tag does not
already exist.

### Consuming a new release

Tags resolve at **lock/build time**, not at runtime — a released version does not
reach an app until it re-resolves:

```bash
cd ../mecon_app        # or ../mecon_etl_dag
uv lock --upgrade-package mecon
```

Commit the resulting `uv.lock`. Dockerised services pick it up on their next
image build.
