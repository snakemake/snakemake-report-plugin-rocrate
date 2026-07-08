import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import spdx_license_list
from rocrate.model import (
    ContextEntity,
    Person,
)
from rocrate.rocrate import ROCrate
from snakemake.logging import logger as snakemake_logger

# Raise errors that will not be handled within this plugin but thrown upwards to
# Snakemake and the user as WorkflowError.
from snakemake_interface_report_plugins.reporter import ReporterBase
from snakemake_interface_report_plugins.settings import ReportSettingsBase


# Optional:
# Define additional settings for your reporter.
# They will occur in the Snakemake CLI as --report-<reporter-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional (or bool) and specify a default value
# of None (or False) or anything else that makes sense in your case.
@dataclass
class ReportSettings(ReportSettingsBase):  # type: ignore[misc]
    run_name: str | None = field(
        default="Workflow Run RO-Crate",
        metadata={
            "help": "Use a descriptive name for your workflow run. "
            "This SHOULD identify the run to humans well enough to disambiguate it from"
            "other workflow runs."
            "If not set, a generic non-descriptive name will be used.",
            "env_var": False,
            "required": False,
        },
    )
    run_description: str | None = field(
        default="Generic Description",
        metadata={
            "help": "Use this field to describe the workflow run in more detail, "
            "including why it was executed, in what context, and why the resulting"
            "dataset matters."
            "If not set, a generic description will be used.",
            "env_var": False,
            "required": False,
        },
    )
    run_license: str | None = field(
        default="CC-BY-4.0",
        metadata={
            "help": "The Crate MUST specify a license. "
            "The license is assumed to apply to any content of the crate,"
            "unless overriden by license on individual File entities."
            "Please specify a SPDX license tag or specify the URL to the license. "
            "For a list of SPDX license tags see:"
            "https://spdx.github.io/spdx-spec/v2.3/SPDX-license-list/ "
            "If not set, CC-BY-4.0",
            "env_var": False,
            "required": False,
        },
    )
    researcher_orcid: str | None = field(
        default=None,
        metadata={
            "help": "Specify the ORCID iD of the person executing the workflow run. "
            "This uniquely identifies the responsible researcher who executed"
            "the workflow run.",
            "env_var": False,
            "required": False,
        },
    )
    researcher_name: str | None = field(
        default=None,
        metadata={
            "help": "Specify the full name of the researcher responsible for"
            "running the workflow. "
            "This will be recorded in the metadata.",
            "env_var": False,
            "required": False,
        },
    )
    organization_ror: str | None = field(
        default=None,
        metadata={
            "help": "Use a ROR ID (Research Organization Registry) to uniquely"
            "identify a research organization. This will be used as the researcher's"
            "affiliation and recorded in the metadata.",
            "env_var": False,
            "required": False,
        },
    )


# Required:
# Implementation of your reporter
class Reporter(ReporterBase):  # type: ignore[misc]
    settings: ReportSettings

    wf_crate: ROCrate
    valid_spdx_identifier: bool
    valid_ror_identifier: bool

    author_props: dict[str, Any]
    org_props: dict[str, Any]

    def __post_init__(self) -> None:
        # initialize additional attributes
        # Do not overwrite the __init__ method as this is kept in control of the base
        # class in order to simplify the update process.
        # See https://github.com/snakemake/snakemake-interface-report-plugins/snakemake_interface_report_plugins/reporter.py # noqa: E501
        # for attributes of the base class.
        # In particular, the settings of above ReportSettings class are accessible via
        # self.settings.
        self.logger: Any = snakemake_logger
        self.author_props = {}
        self.org_props = {"@type": "Organization"}

    def render(self) -> None:
        # Validate Settings
        self.validate_settings()

        # Render the report, using attributes of the base class.

        # Path to main Snakefile!
        self.workflow = self.dag.workflow  # pyright: ignore[reportAttributeAccessIssue]
        main_snakefile = self.dag.workflow.main_snakefile  # pyright: ignore[reportAttributeAccessIssue]
        print("Main snakefile: " + main_snakefile)

        # Base path for the RO-Crate.
        # By default this is the directory containing the Snakefile
        base_path = os.path.dirname(main_snakefile)

        self.wf_crate = ROCrate()

        workflow_path = Path(main_snakefile)
        include_files: list[Path] = []
        self.wf_crate.add_workflow(
            workflow_path,
            workflow_path.name,
            fetch_remote=False,
            main=True,
            lang="snakemake",
            gen_cwl=False,
        )
        # add_workflow(..., main=True) sets mainEntity internally, so this
        # should never be None here.
        # Assert narrows the type for mypy/Pylance
        # and fails loudly if that internal behavior ever changes.
        assert self.wf_crate.mainEntity is not None

        # Set encoding format for main Snakefile
        self.wf_crate.mainEntity["encodingFormat"] = "text/x-python"

        for file_entry in include_files:
            self.wf_crate.add_file(file_entry)

        # Add Workflow Run RO-Crate context to ro-crate-metadata.json
        self.wf_crate.metadata.extra_contexts.append(
            "https://w3id.org/ro/terms/workflow-run/context"
        )

        # Add "conformsTo" statements to RootDataset "./"
        self.wf_crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/ro/wfrun/process/0.5"}
        )
        self.wf_crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/ro/wfrun/workflow/0.5"}
        )
        self.wf_crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/ro/wfrun/provenance/0.5"}
        )
        self.wf_crate.root_dataset.append_to(
            "conformsTo", {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}
        )

        # Add CreativeWork entities that correspond to
        # previosly added "conformsTo" statements
        self.wf_crate.add(
            ContextEntity(
                self.wf_crate,
                "https://w3id.org/ro/wfrun/process/0.5",
                properties={
                    "@type": "CreativeWork",
                    "name": "Process Run Crate",
                    "version": "0.5",
                },
            )
        )
        self.wf_crate.add(
            ContextEntity(
                self.wf_crate,
                "https://w3id.org/ro/wfrun/workflow/0.5",
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow Run Crate",
                    "version": "0.5",
                },
            )
        )
        self.wf_crate.add(
            ContextEntity(
                self.wf_crate,
                "https://w3id.org/ro/wfrun/provenance/0.5",
                properties={
                    "@type": "CreativeWork",
                    "name": "Provenance Run Crate",
                    "version": "0.5",
                },
            )
        )
        self.wf_crate.add(
            ContextEntity(
                self.wf_crate,
                "https://w3id.org/workflowhub/workflow-ro-crate/1.0",
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow RO-Crate",
                    "version": "1.0",
                },
            )
        )

        # Set Root Data Entity name property
        self.wf_crate.name = self.settings.run_name

        # Set Root Data Entity description property
        self.wf_crate.description = self.settings.run_description

        # Set Root Data Entity license property
        self.add_license()

        self.author_props = {"name": self.settings.researcher_name}

        # Add author to RO-Crate
        author = self.wf_crate.add(
            Person(
                self.wf_crate,
                self.settings.researcher_orcid,
                self.author_props,
            )
        )
        self.wf_crate.root_dataset["author"] = author

        # Add an Organization (using ROR ID as @id)
        if self.settings.organization_ror:
            org = ContextEntity(
                self.wf_crate,
                identifier=self.settings.organization_ror,
                properties=self.org_props,
            )
            self.wf_crate.add(org)
            self.author_props["affiliation"] = {"@id": org.id}
            # Add publisher to RO-Crate
            # https://www.researchobject.org/ro-crate/specification/1.1/contextual-entities.html#publisher
            self.wf_crate.root_dataset["publisher"] = org
        else:
            self.wf_crate.root_dataset["publisher"] = author

        # TODO Add workflow input files to metadata

        # TODO Add workflow output files to metadata
        # outputs = [ofile for job in self.jobs for ofile in job.output]

        # Add workflow run as CreateAction
        # Snakemake (to my knowledge) does not create an unique identifier for a run
        start_times = [job_record.starttime for job_record in self.jobs]
        end_times = [job_record.endtime for job_record in self.jobs]

        earliest_start = min(start_times)
        latest_end = max(end_times)

        # convert to local time with timezone info. Format as ISO 8601 with offset
        earliest_start_dt = (
            datetime.fromtimestamp(earliest_start).astimezone().isoformat()
        )
        latest_end_dt = datetime.fromtimestamp(latest_end).astimezone().isoformat()

        run_id = str(uuid.uuid4())
        self.wf_crate.add(
            ContextEntity(
                self.wf_crate,
                ("#" + run_id),
                properties={
                    "@type": "CreateAction",
                    "name": "Snakemake workflow run " + run_id,
                    "startTime": earliest_start_dt,
                    "endTime": latest_end_dt,
                    "instrument": {"@id": self.wf_crate.mainEntity.id},
                },
            )
        )

        # Write only metadata file
        self.wf_crate.metadata.write(base_path)

        print("results: " + str(self.results))
        print("jobs: " + str(self.jobs))
        print("settings: " + str(self.settings))
        print("dag.workflow: " + str(self.workflow))

        print(
            "self.dag.workflow.config_settings: " + str(self.workflow.config_settings)
        )

    def validate_settings(self) -> None:
        # Check valid SPDX identifier.
        self.valid_spdx_identifier = (
            self.settings.run_license in spdx_license_list.LICENSES
        )
        if not self.valid_spdx_identifier:
            self.logger.warning(
                "License '%s' is not a valid SPDX identifier. "
                "Consider using one from https://spdx.org/licenses/",
                self.settings.run_license,
            )

        # Check valid ROR identifier
        ror_id = (
            self.settings.organization_ror
        )  # you can also pass "https://ror.org/04fa4r504"
        if ror_id:
            ror_url = f"https://api.ror.org/organizations/{ror_id.split('/')[-1]}"
            try:
                resp = requests.get(ror_url)
                resp.raise_for_status()
                self.valid_ror_identifier = True

                org = resp.json()
                # Search for an URL (links -> type: "website")
                for link in org["links"]:
                    if link["type"] == "website":
                        self.org_props["url"] = link["value"]

                # Search for the name (entry in "names" with type "ror_display", "label"
                for name in org["names"]:
                    if "ror_display" in name["types"]:
                        self.org_props["name"] = name["value"]
            except requests.exceptions.HTTPError as e:
                self.logger.warning(f"Invalid ROR identifier, HTTP error: {e}")
                self.valid_ror_identifier = False
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"ROR lookup failed: {e}")
                self.valid_ror_identifier = False
        else:
            self.valid_ror_identifier = False

        # self.org_props = {"name": self.settings.researcher_name}

    def add_license(self) -> None:
        run_license = self.settings.run_license or "CC-BY-4.0"

        if self.valid_spdx_identifier:
            self.wf_crate.license = {"@id": "http://spdx.org/licenses/" + run_license}
            self.wf_crate.add(
                ContextEntity(
                    self.wf_crate,
                    self.wf_crate.license,
                    properties={
                        "@type": "CreativeWork",
                        "name": spdx_license_list.LICENSES[run_license].name,
                    },
                )
            )
        else:
            self.wf_crate.license = run_license
