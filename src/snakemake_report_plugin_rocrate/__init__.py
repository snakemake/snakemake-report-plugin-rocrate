"""Snakemake report plugin for Provenance Run RO-Crates."""

from dataclasses import dataclass, field

from snakemake.logging import logger as snakemake_logger
from snakemake_interface_report_plugins.reporter import ReporterBase
from snakemake_interface_report_plugins.settings import ReportSettingsBase

from snakemake_report_plugin_rocrate.provenance import ProvenanceBuilder
from snakemake_report_plugin_rocrate.rocrate_builder import (
    DEFAULT_PROVENANCE_RUN_CRATE_DESCRIPTION,
    DEFAULT_PROVENANCE_RUN_CRATE_NAME,
    ProvenanceRunCrateBuilder,
)
from snakemake_report_plugin_rocrate.utils import validate_filename
from snakemake_report_plugin_rocrate.validator import validate_rocrate


@dataclass
class ReportSettings(ReportSettingsBase):  # type: ignore[misc]
    """Settings for provenance extraction and run-level RO-Crate metadata."""

    filename: str = field(
        default="",
        metadata={
            "help": (
                "Output filename stem; the .zip suffix is added automatically. "
                "Ignored in metadata-only mode."
            ),
            "env_var": False,
            "required": False,
            "unparse_func": str,
        },
    )
    metadata_only: bool = field(
        default=False,
        metadata={
            "help": (
                "Write only the ro-crate-metadata.json descriptor next to the "
                "workflow outputs (the current working directory) instead of "
                "packaging the crate as a ZIP archive. No file is copied, so the "
                "recorded relative paths resolve against the real workflow files."
            ),
            "env_var": False,
            "required": False,
        },
    )
    run_name: str = field(
        default=DEFAULT_PROVENANCE_RUN_CRATE_NAME,
        metadata={
            "help": "Descriptive name for the workflow run.",
            "env_var": False,
            "required": False,
        },
    )
    run_description: str = field(
        default=DEFAULT_PROVENANCE_RUN_CRATE_DESCRIPTION,
        metadata={
            "help": "Description of the workflow run and its context.",
            "env_var": False,
            "required": False,
        },
    )
    run_license: str = field(
        default="CC-BY-4.0",
        metadata={
            "help": "SPDX license identifier or license URL for the crate.",
            "env_var": False,
            "required": False,
        },
    )
    main_tool: str = field(
        default="",
        metadata={
            "help": (
                "Optional preferred package for rule tool wrappers. When omitted, "
                "discovered environment tools are direct CreateAction instruments."
            ),
            "env_var": False,
            "required": False,
        },
    )
    validation_severity: str = field(
        default="REQUIRED",
        metadata={
            "help": (
                "Lowest RO-Crate validation level to inspect; only REQUIRED issues fail the report."
            ),
            "choices": ["REQUIRED", "RECOMMENDED", "OPTIONAL"],
            "env_var": False,
            "required": False,
        },
    )
    researcher_orcid: str = field(
        default="",
        metadata={
            "help": "ORCID iD of the person executing the workflow.",
            "env_var": False,
            "required": False,
        },
    )
    researcher_name: str = field(
        default="",
        metadata={
            "help": "Full name of the person executing the workflow.",
            "env_var": False,
            "required": False,
        },
    )
    agent_orcid: str = field(
        default="",
        metadata={
            "help": "ORCID URL of the person responsible for the workflow run.",
            "env_var": False,
            "required": False,
        },
    )
    agent_name: str = field(
        default="",
        metadata={
            "help": "Full name of the person responsible for the workflow run.",
            "env_var": False,
            "required": False,
        },
    )
    organization_ror: str = field(
        default="",
        metadata={
            "help": "ROR identifier for the researcher's organization.",
            "env_var": False,
            "required": False,
        },
    )
    organization_name: str = field(
        default="",
        metadata={
            "help": "Full name of the researcher's organization.",
            "env_var": False,
            "required": False,
        },
    )
    organization_url: str = field(
        default="",
        metadata={
            "help": "Website URL of the researcher's organization.",
            "env_var": False,
            "required": False,
        },
    )


class Reporter(ReporterBase):  # type: ignore[misc]
    """Generate and validate a Provenance Run RO-Crate."""

    settings: ReportSettings
    external_directory_name = "_EXTERNAL"

    def render(self) -> None:

        if self.settings.filename and not self.settings.metadata_only:
            validate_filename(str(self.settings.filename))
        provenance_builder = ProvenanceBuilder(
            jobs=self.jobs,
            dag=self.dag,
            external_directory_name=self.external_directory_name,
            copy_external_files=not self.settings.metadata_only,
        )

        with provenance_builder.workspace():
            provenance = provenance_builder.build()
            crate_builder = ProvenanceRunCrateBuilder(
                dag=self.dag,
                settings=self.settings,
                rules=self.rules,
            )
            crate_path = crate_builder.write(provenance)
            snakemake_logger.info(f"Crate path: {crate_path}")
            validate_rocrate(
                crate_path,
                requirement_severity=self.settings.validation_severity,
            )
