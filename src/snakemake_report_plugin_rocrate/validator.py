"""Validation helpers for generated RO-Crates.

This module centralizes interaction with ``rocrate_validator`` so the rest of
the package can treat validation as a single, well-defined step.
"""

from __future__ import annotations

from pathlib import Path

from rocrate_validator import models, services
from snakemake.logging import logger as snakemake_logger
from snakemake_interface_common.exceptions import WorkflowError


def validate_rocrate(
    rocrate_uri: str | Path,
    profile_identifier: str = "ro-crate-1.1",
    requirement_severity: models.Severity | str = models.Severity.REQUIRED,
) -> None:
    """Validate a crate path against a selected RO-Crate profile.

    Args:
        rocrate_uri: Path to the generated RO-Crate ZIP file or extracted
            directory to validate.
        profile_identifier: Profile token understood by ``rocrate_validator``,
            such as ``ro-crate-1.1`` or ``provenance-run-crate-0.5``.
        requirement_severity: Minimum validation level to enforce. Accepted
            values are ``REQUIRED``, ``RECOMMENDED``, and ``OPTIONAL``.

    Returns:
        None. Successful validation is indicated by the absence of an
        exception.

    Raises:
        WorkflowError: If the severity is invalid or the validator reports an
            issue at the selected level.
    """
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
        profile_identifier=profile_identifier,
        requirement_severity=requirement_severity,
    )

    result = services.validate(settings)

    if result.has_issues():
        message = "RO-Crate is invalid!\n" + "\n".join(
            f"Detected issue of severity {issue.severity.name} with check "
            f'"{issue.check.identifier}": {issue.message}'
            for issue in result.get_issues()
        )
        raise WorkflowError(message)

    snakemake_logger.info("RO-Crate validation succeeded for profile %s.", profile_identifier)
