"""Job and software-tool helpers for provenance building."""

from typing import Any

from snakemake_report_plugin_rocrate.jsonld import JsonLdNode


class JobMetadataHelpers:
    """Helpers that read Snakemake job metadata and resolve tool nodes."""

    def _job_input_files(self, job) -> list[str]:
        """Collect input file paths for a job from the DAG."""
        return [
            file_path
            for dag_job in self.dag.jobs
            if dag_job.jobid == job.job.jobid
            for file_path in dag_job.input
        ]

    def _job_conda_files(self, job) -> list[Any]:
        """Collect conda environment descriptors for a job."""
        return [dag_job.conda_env for dag_job in self.dag.jobs if dag_job.jobid == job.job.jobid]

    def _job_shell_commands(self, job) -> list[str]:
        """Collect shell commands associated with a job."""
        return [
            dag_job.shellcmd
            for dag_job in self.dag.jobs
            if dag_job.jobid == job.job.jobid and dag_job.shellcmd
        ]

    def _method_optional_fields(self, job) -> JsonLdNode:
        """Build optional method properties inferred from a job."""
        optional_fields: JsonLdNode = {}
        tools = self._job_tools(job)
        if tools:
            optional_fields["implemented by"] = [{"@id": tool["@id"]} for tool in tools]
        return optional_fields

    def _job_tools(self, job) -> list[JsonLdNode]:
        """Resolve software-tool nodes associated with a job."""
        tools: list[JsonLdNode] = []
        for environment in self._job_conda_files(job):
            if not environment:
                continue
            if environment in self.state.conda_tools_cache:
                tools = self.state.conda_tools_cache[environment]
                continue
            tools = self._add_tools(environment)
            self.state.conda_tools_cache[environment] = tools
        return tools

    def _add_tools(self, environment: Any) -> list[JsonLdNode]:
        """Register tool nodes from installed packages in a job environment."""
        tools_list = []
        packages = self.tool_resolver.installed_packages(environment)
        if packages:
            for name, package in packages.items():
                version = package["version"]
                key = f"{name}=={version}"
                if key not in self.state.tools:
                    item = {
                        "@id": f"local:tool_{self.state.tool_counter}",
                        "@type": "schema:SoftwareApplication",
                        "label": name,
                        "url": package.get("url"),
                        "softwareVersion": version,
                    }
                    self.state.tools[key] = item
                    self.state.tool_counter += 1
                    tools_list.append(item)
                else:
                    tools_list.append(self.state.tools[key])
        return tools_list
