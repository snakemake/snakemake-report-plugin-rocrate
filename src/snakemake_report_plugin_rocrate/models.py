"""Shared dataclasses used across provenance extraction and crate building.

These classes carry the in-memory representation of extracted provenance and
the mutable state used while constructing it.
"""

from dataclasses import dataclass, field
from typing import Any

from snakemake_report_plugin_rocrate.jsonld import JsonLdNode, JsonLdNodeMap


@dataclass(frozen=True)
class CrateFile:
    """File scheduled to be copied into the generated RO-Crate.

    Attributes:
        source_path: Original file path on disk.
        dest_path: Destination path inside the RO-Crate.
        name: Display name recorded in metadata.
        encoding_format: MIME type or encoding format stored for the file.
    """

    source_path: str
    dest_path: str
    name: str
    encoding_format: str


@dataclass
class ProvenanceResult:
    """Final provenance payload consumed by RO-Crate builders.

    Attributes:
        file_nodes: Mapping of file paths to JSON-LD file nodes.
        actions: Mapping of action labels to action nodes.
        methods: Mapping of method IDs to method nodes.
        tools: Mapping of tool names or IDs to tool nodes.
        supplemental_files: Files that should be included in the final crate in
            addition to the main provenance serialization.
        workflow_run_action_id: Identifier of the workflow-level action.
    """

    file_nodes: JsonLdNodeMap
    actions: JsonLdNodeMap
    methods: JsonLdNodeMap
    tools: JsonLdNodeMap
    supplemental_files: list[CrateFile] = field(default_factory=list)
    workflow_run_action_id: str = ""


@dataclass
class ProvenanceState:
    """Mutable in-memory state accumulated while building provenance.

    Attributes:
        actions: Action nodes collected so far.
        methods: Method nodes collected so far.
        tools: Tool nodes collected so far.
        supplemental_files: Supplemental files queued for crate inclusion.
        conda_tools_cache: Cache from conda descriptors to extracted tool nodes.
        tool_counter: Counter used to generate unique tool IDs.
        workflow_run_action_id: Workflow-level action identifier.
    """

    actions: JsonLdNodeMap = field(default_factory=dict)
    methods: JsonLdNodeMap = field(default_factory=dict)
    tools: JsonLdNodeMap = field(default_factory=dict)
    supplemental_files: dict[str, CrateFile] = field(default_factory=dict)
    conda_tools_cache: dict[Any, list[JsonLdNode]] = field(default_factory=dict)
    tool_counter: int = 0
    workflow_run_action_id: str = ""
