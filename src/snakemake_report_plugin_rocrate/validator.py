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
    if incomplete:
        identifiers = sorted(check.identifier for check in incomplete)
        raise WorkflowError(f"RO-Crate validation checks did not complete: {identifiers}")
    if result.has_issues():
        message = "RO-Crate is invalid!\n" + "\n".join(
            f"Detected issue of severity {issue.severity.name} with check "
            f'"{issue.check.identifier}": {issue.message}'
            for issue in result.get_issues()
        )
        raise WorkflowError(message)
    snakemake_logger.info("RO-Crate validation succeeded for profile provenance-run-crate-0.5.")
