"""Build a Provenance Run RO-Crate from collected Snakemake execution data.

This module converts :class:`~snakemake_report_plugin_rocrate.models.ProvenanceResult`
instances into concrete RO-Crate ZIP archives.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import spdx_license_list
from rocrate.model import ContextEntity, Person
from rocrate.rocrate import ROCrate
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_report_plugin_rocrate.jsonld import (
    JsonLdNodeMap,
    as_list,
    crate_safe_id,
    reference_id,
)
from snakemake_report_plugin_rocrate.models import ProvenanceResult
from snakemake_report_plugin_rocrate.utils import get_mime_type

PROVENANCE_RUN_CRATE_PROFILE = "provenance-run-crate-0.5"
WORKFLOW_RUN_CONTEXT = "https://w3id.org/ro/terms/workflow-run/context"
DEFAULT_PROVENANCE_RUN_CRATE_NAME = "Snakemake Provenance Run"
DEFAULT_PROVENANCE_RUN_CRATE_DESCRIPTION = "RO-Crate describing a Snakemake workflow run."


class ProvenanceRunCrateBuilder:
    """Transform collected workflow data into a Provenance Run RO-Crate."""

    def __init__(
        self,
        dag: any,
        settings,
        rules: dict[str, Any] | None = None,
        ro_crate_version: str = "1.1",
        default_output_stem: str = "ro-crate",
    ):
        """Initialize shared builder configuration and an empty crate.

        Args:
            settings: Snakemake report plugin settings object.
            rules: Mapping of Snakemake rule names to rule records.
            ro_crate_version: RO-Crate version string used to initialize the
                crate object.
            default_output_stem: Default filename stem used when the user does
                not provide an explicit output name.
        """
        self.settings = settings
        self.dag = dag
        self.rules = rules or {}
        self.ro_crate_version = ro_crate_version
        self.default_output_stem = default_output_stem
        self.crate = ROCrate(version=self.ro_crate_version)
        self.main_tool_id: str | None = None

    def write(self, provenance: ProvenanceResult) -> str:
        """Build the crate and write it to a ZIP archive.

        Args:
            provenance: Provenance payload extracted from the workflow run.

        Returns:
            The path to the written ZIP archive.
        """
        self.build(provenance)
        return self._write_zip()

    def _write_zip(self) -> str:
        """Write the current crate to its resolved output path.

        Returns:
            The output path passed to :meth:`ROCrate.write_zip`.
        """
        crate_path = self._output_path()
        self.crate.write_zip(crate_path)
        return crate_path

    def _output_path(self) -> str:
        """Resolve the output ZIP path for the generated crate.

        Returns:
            The ZIP filename for the generated crate.
        """
        if self.settings.filename:
            return f"{self.settings.filename}.zip"
        return f"{self.default_output_stem}.zip"

    def _apply_rocrate_settings(self) -> None:
        """Apply user-provided name, description, and license values.

        Returns:
            None. The method mutates the root dataset metadata in ``self.crate``.
        """
        self.crate.name = self.settings.run_name
        self.crate.description = self.settings.run_description
        self._apply_license()
        self._apply_responsibility_metadata()

    def _apply_license(self) -> None:
        """Apply a URL, free-text value, or expanded SPDX license."""
        run_license = self.settings.run_license or "CC-BY-4.0"
        if run_license not in spdx_license_list.LICENSES:
            self.crate.license = run_license
            return

        license_id = f"http://spdx.org/licenses/{run_license}"
        self.crate.license = {"@id": license_id}
        if not self.crate.get(license_id):
            self.crate.add(
                ContextEntity(
                    self.crate,
                    license_id,
                    properties={
                        "@type": "CreativeWork",
                        "name": spdx_license_list.LICENSES[run_license].name,
                    },
                )
            )

    def _apply_responsibility_metadata(self) -> None:
        """Add the optional researcher, affiliation, and publisher entities."""
        researcher_name = self.settings.researcher_name
        researcher_orcid = self.settings.researcher_orcid
        organization_ror = self.settings.organization_ror
        organization_name = self.settings.organization_name
        organization_url = self.settings.organization_url
        if not (
            researcher_name
            or researcher_orcid
            or organization_ror
            or organization_name
            or organization_url
        ):
            return

        organization = None
        if organization_ror or organization_name or organization_url:
            organization_properties = {"@type": "Organization"}
            if organization_name:
                organization_properties["name"] = organization_name
            if organization_url:
                organization_properties["url"] = organization_url
            organization = self.crate.add(
                ContextEntity(
                    self.crate,
                    organization_ror or "#organization",
                    properties=organization_properties,
                )
            )

        author_properties: dict[str, Any] = {}
        if researcher_name:
            author_properties["name"] = researcher_name
        if organization is not None:
            author_properties["affiliation"] = {"@id": organization.id}

        author = None
        if researcher_name or researcher_orcid:
            author = self.crate.add(
                Person(
                    self.crate,
                    researcher_orcid or "#researcher",
                    author_properties,
                )
            )
            self.crate.root_dataset["author"] = author

        if organization is not None:
            self.crate.root_dataset["publisher"] = organization
        elif author is not None:
            self.crate.root_dataset["publisher"] = author

    def _add_supplemental_files(self, provenance: ProvenanceResult) -> None:
        """Add supplemental files gathered during provenance extraction.

        Args:
            provenance: Provenance payload containing supplemental file
                descriptors.

        Returns:
            None. Files are added directly to ``self.crate``.
        """
        for file in provenance.supplemental_files:
            self.crate.add_file(
                file.source_path,
                dest_path=file.dest_path,
                properties={
                    "name": file.name,
                    "encodingFormat": file.encoding_format,
                },
            )

    def _add_data_files(self, file_nodes: JsonLdNodeMap) -> dict[str, str]:
        """Add file entities and map source node IDs to crate paths.

        Args:
            file_nodes: Mapping from original file paths to provenance file
                nodes.

        Returns:
            A mapping from provenance ``@id`` values to crate file IDs.
        """
        file_id_map: dict[str, str] = {}
        for file_path, file_node in file_nodes.items():
            if Path(file_path).is_absolute():
                continue  # Skip absolute paths; they cannot be added to the crate.
            self.crate.add_file(
                file_path,
                dest_path=file_path,
                properties={
                    "name": file_node.get("label", file_path),
                    "encodingFormat": get_mime_type(file_path),
                },
            )
            source_id = file_node.get("@id")
            if source_id:
                file_id_map[source_id] = file_path
        return file_id_map

    def build(self, provenance: ProvenanceResult) -> None:
        """Populate a provenance run crate from extracted provenance data.

        Args:
            provenance: Provenance payload extracted from the workflow run.

        Returns:
            None. The crate is mutated in place.
        """
        self._configure_metadata()
        self._add_supplemental_files(provenance)
        file_id_map = self._add_data_files(provenance.file_nodes)
        tool_id_map = self._add_tools(provenance.tools)
        fallback_tool_id = self._ensure_default_software_application()
        step_ids = self._add_how_to_steps(fallback_tool_id)
        workflow_id = self._add_workflow(fallback_tool_id, step_ids)
        self._add_actions(
            provenance=provenance,
            file_id_map=file_id_map,
            tool_id_map=tool_id_map,
            fallback_tool_id=fallback_tool_id,
            workflow_id=workflow_id,
        )
        self._add_agent_to_workflow_action()
        control_action_ids = self._add_control_actions(provenance, step_ids)
        self._add_organize_action(fallback_tool_id, control_action_ids)
        self._add_profile_creative_works()

    def _configure_metadata(self) -> None:
        """Set metadata fields required by the workflow run profiles.

        Returns:
            None. The method mutates the crate metadata and root dataset.
        """
        self.crate.metadata.extra_contexts.append(WORKFLOW_RUN_CONTEXT)

        self._apply_rocrate_settings()
        self.crate.metadata["conformsTo"] = [
            {"@id": "https://w3id.org/ro/crate/1.1"},
            {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"},
        ]
        self.crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/ro/wfrun/process/0.5"}
        )
        self.crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/ro/wfrun/workflow/0.5"}
        )
        self.crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/ro/wfrun/provenance/0.5"}
        )
        self.crate.root_dataset.append_to(
            "conformsTo",
            {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"},
        )

    def _add_tools(self, tools: dict[str, dict[str, Any]]) -> dict[str, str]:
        """Add tool entities and map source tool IDs to crate IDs.

        Args:
            tools: Provenance tool-node mapping.

        Returns:
            A mapping from provenance tool IDs to crate tool IDs.
        """
        tool_id_map: dict[str, str] = {}
        for tool_node in tools.values():
            source_id = tool_node.get("@id")
            crate_id = crate_safe_id(source_id)
            properties = {
                "@type": "SoftwareApplication",
                "name": tool_node.get("label", crate_id),
            }
            if tool_node.get("softwareVersion"):
                properties["softwareVersion"] = tool_node["softwareVersion"]
            self.crate.add(
                ContextEntity(
                    self.crate,
                    crate_id,
                    properties=properties,
                )
            )
            if source_id:
                tool_id_map[source_id] = crate_id
        self._configure_main_tool(tools, tool_id_map)
        return tool_id_map

    def _configure_main_tool(
        self,
        tools: dict[str, dict[str, Any]],
        tool_id_map: dict[str, str],
    ) -> None:
        """Select the primary tool and attach all others as requirements."""
        if not tool_id_map:
            return

        requested_name = self.settings.main_tool.strip()
        matching_ids = [
            tool_id_map[source_id]
            for tool_node in tools.values()
            if (source_id := tool_node.get("@id")) in tool_id_map
            and str(tool_node.get("label", "")).casefold() == requested_name.casefold()
        ]

        if requested_name and not matching_ids:
            available = ", ".join(sorted(tools))
            raise WorkflowError(
                f"Main tool '{requested_name}' was not found. Available tools: {available}."
            )
        if not requested_name and len(tool_id_map) > 1:
            available = ", ".join(sorted(tools))
            raise WorkflowError(
                "Multiple software tools were found. Select the primary one with "
                f"--report-rocrate-main-tool. Available tools: {available}."
            )

        self.main_tool_id = matching_ids[0] if matching_ids else next(iter(tool_id_map.values()))
        main_tool = self.crate.get(self.main_tool_id)
        requirements = [
            {"@id": tool_id} for tool_id in tool_id_map.values() if tool_id != self.main_tool_id
        ]
        if main_tool is not None and requirements:
            main_tool["softwareRequirements"] = requirements

    def _ensure_default_software_application(self) -> str:
        """Ensure the crate contains a fallback Snakemake software entity.

        Returns:
            The crate identifier of the fallback Snakemake software entity.
        """
        software_id = "#snakemake"
        if not self.crate.get(software_id):
            self.crate.add(
                ContextEntity(
                    self.crate,
                    software_id,
                    properties={
                        "@type": "SoftwareApplication",
                        "name": "Snakemake",
                    },
                )
            )
        return software_id

    def _add_actions(
        self,
        provenance: ProvenanceResult,
        file_id_map: dict[str, str],
        tool_id_map: dict[str, str],
        fallback_tool_id: str,
        workflow_id: str | None,
    ) -> None:
        """Translate actions into RO-Crate action entities.

        Args:
            provenance: Complete provenance payload.
            file_id_map: Mapping from provenance file IDs to crate file IDs.
            tool_id_map: Mapping from provenance tool IDs to crate tool IDs.
            fallback_tool_id: Crate ID of the fallback software entity.
            workflow_id: Crate ID of the main workflow entity, when present.

        Returns:
            None. Action, value, and parameter entities are added to the crate.
        """
        file_nodes_by_id = {
            file_node["@id"]: file_node
            for file_node in provenance.file_nodes.values()
            if file_node.get("@id")
        }
        methods_by_id = {
            method_node["@id"]: method_node
            for method_node in provenance.methods.values()
            if method_node.get("@id")
        }
        action_refs: list[dict[str, str]] = []

        for action_node in provenance.actions.values():
            if action_node.get("@type") != "action":
                continue

            action_id = crate_safe_id(action_node.get("@id"))
            input_parameters = []
            output_parameters = []

            for direction, source_key, target in (
                ("input", "has input", input_parameters),
                ("output", "has output", output_parameters),
            ):
                self._add_formal_parameters(
                    action_id=action_id,
                    direction=direction,
                    file_refs=as_list(action_node.get(source_key)),
                    file_id_map=file_id_map,
                    file_nodes_by_id=file_nodes_by_id,
                    workflow_id=workflow_id,
                    target=target,
                )

            self._add_action(
                action_node=action_node,
                action_id=action_id,
                input_parameters=input_parameters,
                output_parameters=output_parameters,
                methods_by_id=methods_by_id,
                tool_id_map=tool_id_map,
                fallback_tool_id=fallback_tool_id,
                workflow_id=workflow_id,
            )
            action_refs.append({"@id": action_id})

        if action_refs:
            self.crate.root_dataset.append_to("mentions", action_refs)

    def _add_organize_action(
        self,
        fallback_tool_id: str,
        control_action_ids: list[str] | None = None,
    ) -> None:
        """Add the action representing orchestration by Snakemake."""
        action_id = "#snakemake-organize-action"
        properties: dict[str, Any] = {
            "@type": "OrganizeAction",
            "name": "Snakemake workflow orchestration",
            "instrument": {"@id": fallback_tool_id},
            "result": {"@id": "#action_workflow_run"},
        }
        create_actions = [
            entity for entity in self.crate.get_entities() if entity.type == "CreateAction"
        ]
        start_times = [
            start_time for action in create_actions if (start_time := action.get("startTime"))
        ]
        end_times = [end_time for action in create_actions if (end_time := action.get("endTime"))]
        if start_times:
            properties["startTime"] = min(start_times)
        if end_times:
            properties["endTime"] = max(end_times)
        if control_action_ids:
            properties["object"] = [
                {"@id": control_action_id} for control_action_id in control_action_ids
            ]
        self.crate.add(
            ContextEntity(
                self.crate,
                action_id,
                properties=properties,
            )
        )

    def _add_agent_to_workflow_action(self) -> None:
        """Link the optional agent to the workflow-run CreateAction."""
        workflow_action = self.crate.get("#action_workflow_run")
        if workflow_action is None:
            return
        agent = self._add_agent()
        if agent is not None:
            workflow_action["agent"] = agent

    def _add_agent(self) -> Person | None:
        """Add the optional person responsible for the workflow run."""
        agent_orcid = str(getattr(self.settings, "agent_orcid", "")).strip()
        agent_name = str(getattr(self.settings, "agent_name", "")).strip()
        if not agent_orcid and not agent_name:
            return None
        if not agent_orcid or not agent_name:
            raise WorkflowError(
                "Both --report-rocrate-agent-orcid and "
                "--report-rocrate-agent-name must be provided together."
            )
        parsed_orcid = urlparse(agent_orcid)
        if parsed_orcid.scheme not in {"http", "https"} or not parsed_orcid.netloc:
            raise WorkflowError("--report-rocrate-agent-orcid must be an HTTP(S) URL.")

        existing_agent = self.crate.get(agent_orcid)
        if existing_agent is not None:
            existing_agent["name"] = agent_name
            return existing_agent
        return self.crate.add(Person(self.crate, agent_orcid, {"name": agent_name}))

    def _add_control_actions(
        self,
        provenance: ProvenanceResult,
        step_ids: list[str],
    ) -> list[str]:
        """Link rule execution actions to their matching workflow steps."""
        steps_by_name = {
            str(step["name"]): step_id
            for step_id in step_ids
            if (step := self.crate.get(step_id)) is not None
        }
        step_names = sorted(steps_by_name, key=len, reverse=True)
        control_action_ids = []

        for action_node in provenance.actions.values():
            create_action_id = crate_safe_id(action_node.get("@id"))
            if create_action_id == "#action_workflow_run":
                continue
            action_name = str(action_node.get("label", create_action_id))
            step_name = next(
                (
                    name
                    for name in step_names
                    if action_name == name or action_name.startswith(f"{name}_")
                ),
                None,
            )
            if step_name is None:
                continue

            control_action_id = f"#control-{create_action_id.removeprefix('#')}"
            self.crate.add(
                ContextEntity(
                    self.crate,
                    control_action_id,
                    properties={
                        "@type": "ControlAction",
                        "name": f"Control {action_name}",
                        "instrument": {"@id": steps_by_name[step_name]},
                        "object": {"@id": create_action_id},
                    },
                )
            )
            control_action_ids.append(control_action_id)
        return control_action_ids

    def _add_how_to_steps(self, fallback_tool_id: str) -> list[str]:
        """Add one workflow description step for each Snakemake rule."""
        step_ids = []
        tool_id = self.main_tool_id or fallback_tool_id
        for position, (rule_name, rule) in enumerate(self.rules.items(), start=1):
            step_id = f"#how-to-step-{rule_name}"
            properties: dict[str, Any] = {
                "@type": "HowToStep",
                "name": rule.name or rule_name,
                "position": position,
                "workExample": {"@id": tool_id},
            }
            if rule.source:
                properties["text"] = rule.source
            self.crate.add(
                ContextEntity(
                    self.crate,
                    step_id,
                    properties=properties,
                )
            )
            step_ids.append(step_id)
        return step_ids

    def _add_formal_parameters(
        self,
        action_id: str,
        direction: str,
        file_refs: list[Any],
        file_id_map: dict[str, str],
        file_nodes_by_id: dict[str, dict[str, Any]],
        workflow_id: str | None,
        target: list[dict[str, str]],
    ) -> None:
        """Create action value and formal parameter entities for an edge list.

        Args:
            action_id: Crate action identifier to which the values belong.
            direction: Parameter direction such as ``input`` or ``output``.
            file_refs: File references taken from provenance action nodes.
            file_id_map: Mapping from provenance file IDs to crate file IDs.
            file_nodes_by_id: File-node lookup keyed by provenance ``@id``.
            workflow_id: Crate ID of the main workflow entity, when present.
            target: List that receives the generated value references.

        Returns:
            None. Generated references are appended to ``target``.
        """
        for index, file_ref in enumerate(file_refs, start=1):
            file_ref_id = reference_id(file_ref)
            if not file_ref_id:
                continue
            parameter = self._add_formal_parameter(
                action_id=action_id,
                direction=direction,
                index=index,
                file_ref_id=file_ref_id,
                file_id_map=file_id_map,
                file_nodes_by_id=file_nodes_by_id,
                workflow_id=workflow_id,
            )
            parameter_id = parameter.id
            value_ref = self._link_action_value_to_parameter(
                action_id=action_id,
                direction=direction,
                index=index,
                file_ref=file_ref,
                file_ref_id=file_ref_id,
                parameter_id=parameter_id,
                file_id_map=file_id_map,
                file_nodes_by_id=file_nodes_by_id,
            )
            target.append(value_ref)

    def _add_formal_parameter(
        self,
        action_id: str,
        direction: str,
        index: int,
        file_ref_id: str,
        file_id_map: dict[str, str],
        file_nodes_by_id: dict[str, dict[str, Any]],
        workflow_id: str | None,
    ) -> Any:
        """Add a formal parameter entity for an action edge.

        Args:
            action_id: Crate action identifier that owns the parameter.
            direction: Parameter direction such as ``input`` or ``output``.
            index: One-based position within the direction-specific parameter
                list.
            file_ref_id: Provenance identifier of the referenced file.
            file_id_map: Mapping from provenance file IDs to crate file IDs.
            file_nodes_by_id: File-node lookup keyed by provenance ``@id``.
            workflow_id: Crate ID of the main workflow entity, when present.

        Returns:
            The created RO-Crate FormalParameter entity.
        """
        file_entity_id = file_id_map.get(file_ref_id, crate_safe_id(file_ref_id))
        file_node = file_nodes_by_id.get(file_ref_id, {})
        name = file_node.get("label", file_entity_id)
        action_slug = action_id.removeprefix("#")
        parameter = self.crate.add_formal_parameter(
            name=name,
            additionalType="File",
            identifier=f"#{action_slug}-{direction}-{index}",
        )
        if workflow_id:
            workflow = self.crate.get(workflow_id)
            if workflow is not None:
                workflow.append_to(direction, {"@id": parameter.id})
        return parameter

    def _link_action_value_to_parameter(
        self,
        action_id: str,
        direction: str,
        index: int,
        file_ref: Any,
        file_ref_id: str,
        parameter_id: str,
        file_id_map: dict[str, str],
        file_nodes_by_id: dict[str, dict[str, Any]],
    ) -> dict[str, str]:
        """Link the action-side value node to its formal parameter.

        Args:
            action_id: Crate action identifier that owns the value.
            direction: Parameter direction such as ``input`` or ``output``.
            index: One-based position within the direction-specific value list.
            file_ref: Original value reference from the provenance action node.
            file_ref_id: Provenance identifier of the referenced value.
            parameter_id: Crate identifier of the formal parameter.
            file_id_map: Mapping from provenance file IDs to crate file IDs.
            file_nodes_by_id: File-node lookup keyed by provenance ``@id``.

        Returns:
            A JSON-LD reference to the action-side value node.
        """
        if file_ref_id in file_id_map or file_ref_id in file_nodes_by_id:
            file_entity_id = file_id_map.get(file_ref_id, crate_safe_id(file_ref_id))
            file_entity = self.crate.get(file_entity_id)
            if file_entity:
                file_entity.append_to("exampleOfWork", {"@id": parameter_id})
            return {"@id": file_entity_id}

        value_id = self._property_value_id(
            action_id=action_id,
            direction=direction,
            index=index,
        )
        self.crate.add(
            ContextEntity(
                self.crate,
                value_id,
                properties={
                    "@type": "PropertyValue",
                    "name": str(file_ref_id),
                    "value": file_ref_id,
                    "exampleOfWork": {"@id": parameter_id},
                },
            )
        )
        return {"@id": value_id}

    def _property_value_id(self, action_id: str, direction: str, index: int) -> str:
        """Return a stable ID for a non-file action parameter value."""
        action_slug = action_id.removeprefix("#")
        return f"#{action_slug}-{direction}-{index}-value"

    def _add_action(
        self,
        action_node: dict[str, Any],
        action_id: str,
        input_parameters: list[dict[str, str]],
        output_parameters: list[dict[str, str]],
        methods_by_id: dict[str, dict[str, Any]],
        tool_id_map: dict[str, str],
        fallback_tool_id: str,
        workflow_id: str | None,
    ) -> ContextEntity:
        """Add a ``CreateAction`` entity for an action node.

        Args:
            action_node: Provenance action node.
            action_id: Target crate identifier for the action.
            input_parameters: File or PropertyValue references representing
                action inputs.
            output_parameters: File or PropertyValue references representing
                action outputs.
            methods_by_id: Method-node lookup keyed by provenance ``@id``.
            tool_id_map: Mapping from provenance tool IDs to crate tool IDs.
            fallback_tool_id: Crate ID of the fallback software entity.
            workflow_id: Crate ID of the main workflow entity, when present.

        Returns:
            The contextual entity added to the crate.
        """
        properties: dict[str, Any] = {
            "@type": "CreateAction",
            "name": action_node.get("label", action_id),
            "instrument": self._instrument_id_for_action(
                action_node=action_node,
                action_id=action_id,
                methods_by_id=methods_by_id,
                tool_id_map=tool_id_map,
                fallback_tool_id=fallback_tool_id,
                workflow_id=workflow_id,
            ),
        }
        if action_node.get("start time"):
            properties["startTime"] = action_node["start time"]
        if action_node.get("end time"):
            properties["endTime"] = action_node["end time"]
        if input_parameters:
            properties["object"] = input_parameters
        if output_parameters:
            properties["result"] = output_parameters
        return self.crate.add(
            ContextEntity(
                self.crate,
                action_id,
                properties=properties,
            )
        )

    def _instrument_id_for_action(
        self,
        action_node: dict[str, Any],
        action_id: str,
        methods_by_id: dict[str, dict[str, Any]],
        tool_id_map: dict[str, str],
        fallback_tool_id: str,
        workflow_id: str | None,
    ) -> dict[str, str]:
        """Resolve the workflow or software instrument for an action."""
        if action_id == "#action_workflow_run":
            return {"@id": workflow_id or fallback_tool_id}
        if self.main_tool_id:
            return {"@id": self.main_tool_id}

        method_id = reference_id(action_node.get("realizes method"))
        method_node = methods_by_id.get(method_id, {}) if method_id else {}
        for tool_ref in as_list(method_node.get("implemented by")):
            tool_id = reference_id(tool_ref)
            crate_tool_id = tool_id_map.get(tool_id) if tool_id else None
            if crate_tool_id:
                return {"@id": crate_tool_id}
        return {"@id": fallback_tool_id}

    def _add_workflow(
        self,
        fallback_tool_id: str,
        step_ids: list[str] | None = None,
    ) -> str | None:
        """Add the Snakefile as the main workflow entity when available.

        Args:
            fallback_tool_id: Crate ID of the fallback software entity to use
                when no main tool was discovered.
            step_ids: Crate IDs of the workflow's ``HowToStep`` entities.

        Returns:
            The workflow entity ID, or ``None`` when no Snakefile is available.
        """
        snakefile = self.dag.workflow.main_snakefile

        if not snakefile:
            return None

        workflow_path = Path(snakefile)

        tool_id = self.main_tool_id or fallback_tool_id
        properties: dict[str, Any] = {
            "@type": [
                "File",
                "SoftwareSourceCode",
                "ComputationalWorkflow",
                "HowTo",
            ],
            "hasPart": {"@id": tool_id},
        }
        if step_ids:
            properties["step"] = [{"@id": step_id} for step_id in step_ids]
        workflow = self.crate.add_workflow(
            source=workflow_path,
            dest_path=workflow_path.name,
            lang="snakemake",
            main=True,
            fetch_remote=False,
            properties=properties,
            gen_cwl=False,
        )
        self.crate.mainEntity = {"@id": workflow.id}
        return workflow.id

    def _add_profile_creative_works(self) -> None:
        """Add profile descriptors referenced by ``conformsTo`` statements.

        Returns:
            None. CreativeWork entities are added to the crate in place.
        """
        self.crate.add(
            ContextEntity(
                self.crate,
                "https://w3id.org/ro/wfrun/process/0.5",
                properties={
                    "@type": "CreativeWork",
                    "name": "Process Run Crate",
                    "version": "0.5",
                },
            )
        )
        self.crate.add(
            ContextEntity(
                self.crate,
                "https://w3id.org/ro/wfrun/workflow/0.5",
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow Run Crate",
                    "version": "0.5",
                },
            )
        )
        self.crate.add(
            ContextEntity(
                self.crate,
                "https://w3id.org/ro/wfrun/provenance/0.5",
                properties={
                    "@type": "CreativeWork",
                    "name": "Provenance Run Crate",
                    "version": "0.5",
                },
            )
        )
        self.crate.add(
            ContextEntity(
                self.crate,
                "https://w3id.org/workflowhub/workflow-ro-crate/1.0",
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow RO-Crate",
                    "version": "1.0",
                },
            )
        )
