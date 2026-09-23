"""File and supplemental-resource helpers for provenance building."""

import logging
import os
import shlex
import shutil
from pathlib import Path

from snakemake_report_plugin_rocrate.jsonld import JsonLdNode, JsonLdNodeMap
from snakemake_report_plugin_rocrate.models import CrateFile
from snakemake_report_plugin_rocrate.utils import get_mime_type

logger = logging.getLogger(__name__)


class FileProvenanceHelpers:
    """Helpers that register provenance file nodes and supplemental files."""

    def _add_shell_supplemental_files(self, job) -> None:
        """Register shell scripts referenced by a job as supplemental files."""
        for shell_cmd in self._job_shell_commands(job):
            script_file, _ = self._extract_script_and_files(shell_cmd)
            if not script_file:
                continue
            resolved_shell_path = self._copy_external_relative_files(script_file)
            self._add_supplemental_file(
                resolved_shell_path,
                resolved_shell_path,
                get_mime_type(resolved_shell_path),
            )

    def _populate_action_files(
        self,
        job,
        node: JsonLdNode,
        file_nodes: JsonLdNodeMap,
    ) -> None:
        """Attach input and output file references to a processing action."""
        dag_job = next((item for item in self.dag.jobs if item.jobid == job.job.jobid), None)
        for direction in ("input", "output"):
            values = (
                getattr(dag_job, direction)
                if dag_job is not None
                else (self._job_input_files(job) if direction == "input" else job.output)
            )
            named_slots = {}
            for name, (start, end) in getattr(values, "_get_names", lambda: [])():
                for index in range(start, end if end is not None else start + 1):
                    named_slots[index] = name
            for index, file_path in enumerate(values):
                if not self._is_file(file_path):
                    continue
                file_node = self._add_file(file_path, file_nodes)
                node[f"has {direction}"].append(
                    {
                        "@id": file_node["@id"],
                        "parameter rule": str(job.rule),
                        "parameter slot": named_slots.get(index, str(index + 1)),
                        "parameter named": index in named_slots,
                    }
                )

    def _add_snakefile_supplemental_file(self) -> None:
        """Register the workflow Snakefile as a supplemental file when found."""
        snakefile = self._find_snakefile()
        if not snakefile:
            return
        snakefile_name, snakepath = snakefile
        self._add_supplemental_file(
            snakefile_name,
            snakepath,
            "text/x-python",
        )

    def _add_file(self, file_path: str, file_dict: JsonLdNodeMap) -> JsonLdNode:
        """Register a file node, copying external files into the workspace."""
        resolved_path = self._copy_external_relative_files(file_path)
        if resolved_path not in file_dict:
            file_dict[resolved_path] = {
                "@id": f"local:file_{len(file_dict)}",
                "@type": "cr:FileObject",
                "label": resolved_path,
                "source path": str(Path(file_path).resolve()),
            }
        return file_dict[resolved_path]

    def _add_supplemental_file(
        self, source_path: str, dest_path: str, encoding_format: str
    ) -> None:
        """Register a supplemental file for later inclusion in the crate."""
        self.state.supplemental_files[dest_path] = CrateFile(
            source_path=source_path,
            dest_path=dest_path,
            name=source_path,
            encoding_format=encoding_format,
        )

    def _extract_script_and_files(self, cmd: str) -> tuple[str | None, list[str]]:
        """Parse a shell command and identify likely script and file arguments."""
        interpreters = {
            "python",
            "python3",
            "python2",
            "pypy",
            "pypy3",
            "ruby",
            "perl",
            "node",
            "deno",
            "php",
            "lua",
            "Rscript",
            "R",
            "bash",
            "sh",
            "zsh",
            "ksh",
            "fish",
        }

        try:
            tokens = shlex.split(cmd, posix=True)
        except ValueError:
            return None, []

        if not tokens:
            return None, []

        script_path = None
        file_paths = []

        if Path(tokens[0]).name in interpreters:
            start_idx = 1
            for i, tok in enumerate(tokens[1:], start=1):
                if tok in {"-c", "-e", "--command"} or tok.startswith("-"):
                    continue
                if tokens[i - 1] in {"-c", "-e", "--command"}:
                    # inline code/command, not a file
                    start_idx = i + 1
                    break
                script_path = tok
                start_idx = i + 1
                break
        else:
            first = Path(tokens[0])
            if first.suffix and first.suffix not in {".exe", ".bat", ".cmd"}:
                script_path = str(first)
            start_idx = 1

        for tok in tokens[start_idx:]:
            if tok.startswith("-") or tok in {">", "2>&1"} or tok.isnumeric():
                continue
            if Path(tok).suffix or "/" in tok or tok.startswith(".."):
                file_paths.append(tok)

        return script_path, file_paths

    def _find_snakefile(self):
        """Return the configured Snakefile name and relative path when available."""
        snakefile = self.dag.workflow.main_snakefile
        if not snakefile:
            return None

        snakefile_path = Path(snakefile)
        return snakefile_path.name, os.path.relpath(snakefile_path)

    def _is_file(self, file_name: str) -> bool:
        """Return whether a path currently exists as a regular file."""
        return Path(file_name).is_file()

    def _copy_external_relative_files(self, path_str) -> str:
        """Copy external files into the workspace while preserving structure."""
        original_path = Path(path_str).resolve()
        current_dir = Path.cwd().resolve()

        try:
            return str(original_path.relative_to(current_dir))
        except ValueError:
            pass

        common_root = os.path.commonpath([str(current_dir), str(original_path)])
        relative_structure = Path(original_path).relative_to(common_root)
        target_path = Path(self.external_directory_name) / relative_structure

        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(original_path, target_path)
        except PermissionError:
            if target_path.exists():
                logger.warning(
                    "Cannot overwrite read-only file %s; keeping existing copy.",
                    target_path,
                )
            else:
                raise

        return str(target_path)
