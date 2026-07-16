## Contributing to `snakemake-report-plugin-rocrate`

This guide covers how to get a local development environment running
and how to check your changes before pushing.

### Prerequisites

This project uses [pixi](https://pixi.sh) exclusively for environment and
dependency management.

Install pixi:

```bash
curl -fsSL https://pixi.sh/install.sh | bash    # macOS / Linux
```

### Getting started

```bash
git clone https://github.com/snakemake/snakemake-report-plugin-rocrate.git
cd snakemake-report-plugin-rocrate
pixi install
```

This solves and creates the `dev` environment, including the plugin itself
installed editable. This means changes to `src/` are picked up immediately without
reinstalling.

### Available pixi tasks

All checks run through `pixi run -e dev <task>`:

| Task | What it does |
|---|---|
| `format` | Auto-fix formatting with `ruff format` |
| `format-check` | Check formatting without modifying files |
| `lint` | Run `ruff check` |
| `typecheck` | Run `mypy` against `src/` |
| `test` | Run `pytest` with coverage against `tests/` |
| `build` | Build sdist + wheel via `python -m build` |
| `check-build` | Build, then validate metadata with `twine check` |
| `verify-install` | Install the built wheel and confirm it imports |
| `check` | `format-check` → `lint` → `typecheck` → `test`, in order |
| `ci` | Mimics the full GitHub Actions pipeline locally (see below) |

### Before you push

Please run

```bash
pixi run -e dev check
```

Before pushing changes. This runs format, lint, typecheck, and test against just
the `dev` environment. It is a subset of the full CI pipeline and should finish
quickly.

But you can also run the full local pipeline, which mirrors CI as closely as possible:

```bash
pixi run -e dev ci
```

This runs, in order: formatting, linting, typechecking, a full package
build + metadata validation, an install-and-import check for every
supported Python version (3.11–3.14), and the test suite for each of
those versions.

### Continuous Integration

Every push and pull request to `main` runs the same checks via GitHub
Actions: `formatting`, `linting`, `typecheck`, `build`, `verify-install`
(matrixed across all Python versions supported by current Snakemake release), and `test` (matrixed across the same
versions).

### Commit conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `chore:`, `docs:`, `ci:`, etc.). Commits should be signed
(GPG or SSH).

## A note on typing

Static typing is not currently enforced for the plugin's own modules:

- `ANN` (missing/incorrect type annotations) is disabled for `src/` via a
  per-file ignore in `pyproject.toml`.
- `mypy` errors are suppressed for most of the plugin's own modules via
  `ignore_errors = true` overrides.

This is a deliberate choice for now. Implementation is still actively
taking shape, and requiring fully-typed code at this stage would slow
that down more than it would help. Type annotations are welcome if you
want to add them, but they are currently not required for a contribution
to be accepted.