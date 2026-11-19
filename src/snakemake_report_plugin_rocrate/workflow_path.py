"""Resolve the main Snakefile independently of Snakemake's output directory."""

from pathlib import Path
from typing import Any


def main_snakefile_path(workflow: Any) -> Path | None:
    """Return the source path supplied by Snakemake for the main Snakefile."""
    snakefile = workflow.main_snakefile
    if not snakefile:
        return None

    path = Path(snakefile)
    if path.is_absolute():
        return path

    # Snakemake can change cwd for --directory after locating the Snakefile.
    # Its run directory still points to the directory used to locate it.
    rundir = getattr(workflow, "rundir", None)
    if rundir is not None:
        candidate = Path(rundir) / path
        if candidate.is_file():
            return candidate.resolve()
    return path.resolve()
