"""Read resolved packages from the Conda environment used by a job."""

import json
from pathlib import Path
from typing import Any


class ToolResolver:
    """Resolve installed package metadata from a Snakemake Conda environment."""

    def installed_packages(self, environment: Any) -> dict[str, dict[str, str]]:
        """Return package versions and URLs recorded in the job's Conda prefix.

        A declared environment alone does not establish the installed versions.
        When the prefix is unavailable, no package claims are made.
        """
        if environment is None:
            return {}
        metadata_dir = Path(environment.address) / "conda-meta"
        if not metadata_dir.is_dir():
            return {}

        packages: dict[str, dict[str, str]] = {}
        for record in sorted(metadata_dir.glob("*.json")):
            metadata = json.loads(record.read_text())
            name = metadata.get("name")
            version = metadata.get("version")
            if not name or not version:
                continue
            package = {"version": str(version)}
            url = metadata.get("url")
            if isinstance(url, str) and url.startswith(("https://", "http://")):
                package["url"] = url
            packages[str(name).lower()] = package
        return packages
