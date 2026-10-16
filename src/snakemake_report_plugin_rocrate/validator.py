"""Validation helpers for generated Provenance Run RO-Crates."""

from __future__ import annotations

from pathlib import Path

from rocrate_validator import models, services
from snakemake.logging import logger as snakemake_logger
from snakemake_interface_common.exceptions import WorkflowError


def validate_rocrate(
    rocrate_uri: str | Path,
    requirement_severity: models.Severity | str = models.Severity.REQUIRED,
) -> None:
    """Validate a crate against Provenance Run Crate 0.5."""
    if isinstance(requirement_severity, str):
        try:
            requirement_severity = models.Severity[requirement_severity.strip().upper()]
        except KeyError:
            choices = ", ".join(severity.name for severity in models.Severity)
            raise WorkflowError(
                f"Unknown RO-Crate validation severity '{requirement_severity}'. "
                f"Choose one of: {choices}."
            ) from None

    settings = services.ValidationSettings(
        rocrate_uri=Path(rocrate_uri),
        profile_identifier="provenance-run-crate-0.5",
        requirement_severity=requirement_severity,
    )
    result = services.validate(settings)

    expected = {
        check
        for profile in result.context.profiles
        for requirement in profile.get_requirements(requirement_severity)
        for check in requirement.get_checks()
        if check.severity >= requirement_severity and not check.overridden and not check.deactivated
    }
    incomplete = expected - result.executed_checks - result.skipped_checks
    required_incomplete = sorted(
        check.identifier for check in incomplete if check.severity >= models.Severity.REQUIRED
    )
    if required_incomplete:
        raise WorkflowError(
            f"Required RO-Crate validation checks did not complete: {required_incomplete}"
        )

    required_issues = []
    advisory_issues = []
    for issue in result.get_issues():
        (required_issues if issue.severity >= models.Severity.REQUIRED else advisory_issues).append(
            issue
        )
    if required_issues:
        message = "RO-Crate is invalid: required checks failed.\n" + "\n".join(
            f'REQUIRED check "{issue.check.identifier}": {issue.message}'
            for issue in required_issues
        )
        raise WorkflowError(message)

    if advisory_issues or incomplete:
        snakemake_logger.warning(
            "RO-Crate is valid (all REQUIRED checks passed); "
            f"{len(advisory_issues)} RECOMMENDED/OPTIONAL finding(s) and "
            f"{len(incomplete)} advisory check(s) not evaluated."
        )
    for check in sorted(incomplete, key=lambda item: item.identifier):
        snakemake_logger.warning(
            f"RO-Crate {check.severity.name} check could not be evaluated: {check.identifier}"
        )
    for issue in advisory_issues:
        snakemake_logger.warning(
            f'RO-Crate advisory ({issue.severity.name}, check "{issue.check.identifier}"): '
            f"{issue.message}"
        )
    snakemake_logger.info("RO-Crate validation succeeded for profile provenance-run-crate-0.5.")
