"""Collect Snakemake execution data for Provenance Run RO-Crate generation.

The classes in this module transform Snakemake runtime objects, workflow
metadata into a small internal representation consumed by the RO-Crate builder.
"""

import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from snakemake_report_plugin_rocrate.jsonld import (
    JsonLdNode,
    JsonLdNodeMap,
)
from snakemake_report_plugin_rocrate.models import (
    ProvenanceResult,
    ProvenanceState,
)
from snakemake_report_plugin_rocrate.provenance_files import FileProvenanceHelpers
from snakemake_report_plugin_rocrate.provenance_graph import ProvenanceGraphHelpers
from snakemake_report_plugin_rocrate.provenance_jobs import JobMetadataHelpers
from snakemake_report_plugin_rocrate.tool_resolver import ToolResolver


class ProvenanceBuilder(
    FileProvenanceHelpers,
    JobMetadataHelpers,
    ProvenanceGraphHelpers,
):
    """Build an intermediate provenance graph from Snakemake execution data.

    The builder walks completed jobs, derives action, method, file,
    parameter, and tool nodes, and returns a
    :class:`ProvenanceResult` object containing both the assembled JSON-LD
    document and the registries used to build it.
    """

    def __init__(
        self,
        jobs,
        dag,
        external_directory_name: str = "_EXTERNAL",
        copy_external_files: bool = True,
    ):
        """Initialize the builder with Snakemake runtime objects.

        Args:
            jobs: Iterable of Snakemake job records with timing information.
            dag: Snakemake DAG object used to resolve rule inputs, shell
                commands, and conda environments.
            external_directory_name: Working directory name used for copied
                files that live outside the current report directory.
            copy_external_files: When False, files outside the report directory
                are referenced by their path relative to it instead of being
                copied into a temporary workspace. Used by metadata-only mode,
                which must not copy any file.

        Returns:
            None.
        """
        self.jobs = jobs
        self.dag = dag
        self.external_directory_name = external_directory_name
        self.copy_external_files = copy_external_files
        self.state = ProvenanceState()
        self.tool_resolver = ToolResolver()

    def build(self) -> ProvenanceResult:
        """Build the complete intermediate provenance payload.

        Returns:
            ProvenanceResult: Container with the assembled JSON-LD document,
            supporting node registries and supplemental file records.
        """
        self.state = ProvenanceState()
        sorted_jobs = sorted(self.jobs, key=lambda job: job.starttime)
        file_nodes: JsonLdNodeMap = {}
        config_files = self._add_configfiles()

        self._add_workflow_run_action(sorted_jobs)

        for job in sorted_jobs:
            job_label = f"{job.rule}_{job.job.jobid}"
            action_node = self._create_action_node(job, file_nodes)
            self.state.actions[job_label] = action_node

        self._add_precedes_relations()
        return ProvenanceResult(
            file_nodes=file_nodes,
            actions=self.state.actions,
            methods=self.state.methods,
            tools=self.state.tools,
            supplemental_files=list(self.state.supplemental_files.values()),
            config_files=config_files,
            workflow_run_action_id=self.state.workflow_run_action_id,
        )

    def create_external_directory(self):
        """Create a clean workspace for copied external file references.

        Returns:
            None.
        """
        if not self.copy_external_files:
            return
        target_dir = Path(self.external_directory_name)
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def workspace(self) -> Iterator[None]:
        """Provide a temporary workspace lifecycle around provenance work.

        Yields:
            None: Control returns to the caller while the external workspace is
            available.
        """
        self.create_external_directory()
        try:
            yield
        finally:
            self.clean_data()

    def clean_data(self):
        """Remove temporary workspace content and serialized provenance files.

        Returns:
            None.
        """
        if not self.copy_external_files:
            return
        target_dir = Path(self.external_directory_name)
        if target_dir.exists():
            shutil.rmtree(target_dir)

    def _create_action_node(self, job, file_nodes: JsonLdNodeMap) -> JsonLdNode:
        """Create the action node for one executed Snakemake job.

        Args:
            job: Snakemake job record with rule name, job identifier, outputs,
                and timing information.
            file_nodes: Shared registry of already-created file nodes.

        Returns:
            JsonLdNode: Action node with input/output references and a
            linked method node.
        """
        node = {
            "@id": f"local:action_{job.job.jobid}",
            "@type": "action",
            "label": f"{job.rule}_{job.job.jobid}",
            "rule": str(job.rule),
            "description": "\n".join(self._job_shell_commands(job)) or f"Execute rule {job.rule}",
            "start time": self._get_time_str(job.starttime),
            "end time": self._get_time_str(job.endtime),
            "has input": [],
            "has output": [],
            "realizes method": [],
            "part of": {"@id": self.state.workflow_run_action_id},
        }
        self._add_shell_supplemental_files(job)
        optional_fields = self._method_optional_fields(job)
        self._populate_action_files(
            job=job,
            node=node,
            file_nodes=file_nodes,
        )
        node["realizes method"] = {"@id": self._create_method_node(job, optional_fields)}
        for environment in self._job_conda_files(job):
            source = getattr(environment, "file", None)
            if source and Path(str(source)).is_file():
                source_path = Path(str(source)).resolve()
                environment_path = self._workflow_resource_destination(source_path)
                if environment_path is None:
                    environment_path = self._copy_external_relative_files(str(source_path))
                    source_path = Path(environment_path)
                self._add_supplemental_file(str(source_path), environment_path, "application/yaml")
                node["environment file"] = environment_path
        self._add_snakefile_supplemental_file()
        dag_job = next((item for item in self.dag.jobs if item.jobid == job.job.jobid), None)
        if dag_job is not None and getattr(dag_job.rule, "script", None):
            source = dag_job._path_to_source_file(dag_job.rule.script)
            script_path = Path(source.get_path_or_uri(secret_free=False))
            if not script_path.is_absolute() and not script_path.is_file():
                rundir = getattr(self.dag.workflow, "rundir", None)
                if rundir is not None and (Path(rundir) / script_path).is_file():
                    script_path = Path(rundir) / script_path
            if script_path.is_file():
                script_path = script_path.resolve()
                snakefile = self._find_snakefile()
                try:
                    destination = (
                        str(script_path.relative_to(snakefile[1].parent.resolve()))
                        if snakefile
                        else None
                    )
                except ValueError:
                    destination = None
                if destination is None:
                    destination = self._copy_external_relative_files(str(script_path))
                self._add_supplemental_file(str(script_path), destination, "text/x-python")
                node["script file"] = destination
        return node

    def _create_method_node(self, job, optional_fields: JsonLdNode) -> str:
        """Create and register the method node backing one action.

        Args:
            job: Job whose rule name and identifier determine the method label
                and identifier.
            optional_fields: Additional method properties to merge into the
                created node.

        Returns:
            str: Local identifier of the created method node.
        """
        method_id = f"local:method_{job.rule}_{job.job.jobid}"
        self.state.methods[method_id] = {
            "@id": method_id,
            "@type": "method",
            "label": f"{job.rule}_{job.job.jobid}",
            **optional_fields,
        }
        return method_id
