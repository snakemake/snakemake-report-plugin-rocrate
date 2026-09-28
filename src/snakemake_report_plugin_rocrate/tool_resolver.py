"""Software tool discovery from conda environment definitions."""

import json
import re
from pathlib import Path

import yaml


class ToolResolver:
    """Resolve software tool metadata from conda environment definitions.

    Tool versions can be declared directly in a workflow's environment YAML,
    but sometimes only package names are present. This helper reads the job's
    own conda-meta records to capture installed versions when available.
    """

    def __init__(self) -> None:
        """Initialize conda discovery caches.

        Returns:
            None.
        """
        self.package_urls: dict[str, str] = {}

    def extract_tools_from_yaml(
        self, env_file_content: str, env_path: str | None = None
    ) -> dict[str, str | None]:
        """Extract tool names and versions from a conda environment file.

        Args:
            env_file_content: Text contents of a conda environment YAML file.

        Returns:
            dict[str, str | None]: Mapping from normalized package name to
            discovered version. Versions remain ``None`` when neither the YAML
            file nor the job environment provides one.
        """
        results: dict[str, str | None] = {}
        found_targets = set()
        parsed = yaml.safe_load(env_file_content) or {}
        dependencies = parsed.get("dependencies", [])
        self.package_urls = {}
        channels = parsed.get("channels", [])
        default_channel = channels[0] if channels else "anaconda"

        version_pattern = re.compile(r"([a-zA-Z0-9_.\-]+)([=><!~]+.*)?")

        for dep in dependencies:
            if isinstance(dep, str):
                channel, _, specification = dep.strip().rpartition("::")
                match = version_pattern.fullmatch(specification)

                if not match:
                    continue
                pkg_name = match.group(1).lower()
                version = match.group(2).lstrip("=") if match.group(2) else None
                results[pkg_name] = (
                    version if version and not any(c in version for c in "<>!*~,") else None
                )
                registry = channel or default_channel
                if registry == "defaults":
                    registry = "anaconda"
                if re.fullmatch(r"[a-zA-Z0-9_-]+", str(registry)):
                    self.package_urls[pkg_name] = f"https://anaconda.org/{registry}/{pkg_name}"
                found_targets.add(pkg_name)
            elif isinstance(dep, dict):
                for _, pkgs in dep.items():
                    for pkg in pkgs:
                        match = version_pattern.match(pkg.strip())
                        if not match:
                            continue
                        pkg_name = match.group(1).lower()
                        version = match.group(2).lstrip("=") if match.group(2) else None
                        results[pkg_name] = (
                            version if version and not any(c in version for c in "<>!*~,") else None
                        )
                        self.package_urls[pkg_name] = f"https://pypi.org/project/{pkg_name}/"
                        found_targets.add(pkg_name)

        # Use only the environment attached to this job. A similarly populated
        # unrelated environment does not establish what executed the workflow.
        if env_path:
            for record in (Path(env_path) / "conda-meta").glob("*.json"):
                metadata = json.loads(record.read_text())
                name = metadata.get("name", "").lower()
                if name in found_targets and metadata.get("version"):
                    results[name] = metadata["version"]
                    if metadata.get("url"):
                        self.package_urls[name] = metadata["url"]

        return results
