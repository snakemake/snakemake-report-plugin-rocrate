"""File and supplemental-resource helpers for provenance building."""

import logging
import os
import shlex
import shutil
from pathlib import Path

from snakemake_report_plugin_rocrate.jsonld import JsonLdNode, JsonLdNodeMap
from snakemake_report_plugin_rocrate.models import CrateFile
from snakemake_report_plugin_rocrate.utils import get_mime_type
from snakemake_report_plugin_rocrate.workflow_path import main_snakefile_path

logger = logging.getLogger(__name__)


class FileProvenanceHelpers:
    """Helpers that register provenance file nodes and supplemental files."""

    def _add_configfiles(self) -> list[str]:
        """Register loaded configuration files and return their crate paths."""
        destinations = []
        for configfile in getattr(self.dag.workflow, "configfiles", ()):
            path = Path(configfile)
            if not path.is_file():
                continue
            path = path.resolve()
            try:
                destination = str(path.relative_to(Path.cwd().resolve()))
            except ValueError:
                destination = self._workflow_resource_destination(path)
            if destination is None:
                destination = self._copy_external_relative_files(str(path))
                source = destination
            else:
                source = str(path)
            encoding = (
                "application/yaml"
                if path.suffix.lower() in {".yaml", ".yml"}
                else get_mime_type(str(path))
            )
            self._add_supplemental_file(source, destination, encoding)
            if destination not in destinations:
                destinations.append(destination)
        return destinations

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
        """Register all local Snakefiles loaded by Snakemake."""
        snakefile = self._find_snakefile()
        if not snakefile:
            return
        snakefile_name, snakepath = snakefile
        if not getattr(self, "copy_external_files", True):
            snakefile_name = os.path.relpath(snakepath.resolve(), Path.cwd())
        self._add_supplemental_file(
            str(snakepath),
            snakefile_name,
            "text/x-python",
        )
        workflow = self.dag.workflow
        for source in getattr(workflow, "included", ()):
            path = Path(source.get_path_or_uri(secret_free=False))
            if not path.is_absolute():
                rundir = getattr(workflow, "rundir", None)
                if rundir is not None and (Path(rundir) / path).is_file():
                    path = Path(rundir) / path
            path = path.resolve()
            if path == snakepath.resolve() or not path.is_file():
                continue
            if not getattr(self, "copy_external_files", True):
                destination = os.path.relpath(path, Path.cwd())
            else:
                try:
                    destination = str(path.relative_to(snakepath.parent.resolve()))
                except ValueError:
                    destination = self._copy_external_relative_files(str(path))
            self._add_supplemental_file(str(path), destination, "text/x-python")

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
        """Return the Snakefile archive name and source path when available."""
        snakefile_path = main_snakefile_path(self.dag.workflow)
        if snakefile_path is None:
            return None
        return snakefile_path.name, snakefile_path

    def _is_file(self, file_name: str) -> bool:
        """Return whether a path currently exists as a regular file."""
        return Path(file_name).is_file()

    def _workflow_resource_destination(self, source_path: Path) -> str | None:
        """Place files from the workflow source tree under its root directory."""
        if not getattr(self, "copy_external_files", True):
            return None
        snakefile = main_snakefile_path(self.dag.workflow)
        if snakefile is None:
            return None
        source_root = snakefile.resolve().parent
        try:
            relative_path = source_path.resolve().relative_to(source_root)
        except ValueError:
            return None
        return str(Path(source_root.name) / relative_path)

    def _copy_external_relative_files(self, path_str) -> str:
        """Copy external files into the workspace while preserving structure.

        In metadata-only mode (``copy_external_files`` disabled) nothing is
        copied; the file is instead referenced by its path relative to the
        report directory so the recorded relative path stays valid in place.
        """
        original_path = Path(path_str).resolve()
        current_dir = Path.cwd().resolve()

        try:
            return str(original_path.relative_to(current_dir))
        except ValueError:
            pass

        if not getattr(self, "copy_external_files", True):
            return os.path.relpath(original_path, current_dir)

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
