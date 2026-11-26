"""Validation helpers for generated Provenance Run RO-Crates."""

from __future__ import annotations

from pathlib import Path

from rocrate_validator import models, services
from snakemake.logging import logger as snakemake_logger
from snakemake_interface_common.exceptions import WorkflowError


def _unique_issue_messages(issues, label: str) -> list[str]:
    """Show each finding once, with every check that reported it."""
    checks_by_message: dict[str, list[str]] = {}
    for issue in issues:
        checks = checks_by_message.setdefault(issue.message, [])
        if issue.check.identifier not in checks:
            checks.append(issue.check.identifier)
    lines = []
    for message, checks in checks_by_message.items():
        identifiers = ", ".join(f'"{identifier}"' for identifier in checks)
        noun = "check" if len(checks) == 1 else "checks"
        lines.append(f"{label} {noun} {identifiers}: {message}")
    return lines


def validate_rocrate(
    rocrate_uri: str | Path,
    requirement_severity: models.Severity | str = models.Severity.REQUIRED,
) -> None:
    """Validate a crate against Provenance Run Crate 0.6."""
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
        profile_identifier="provenance-run-crate-0.6",
        requirement_severity=requirement_severity,
    )
    result = services.validate(settings)

    # The profile may define internal heuristic checks that are intentionally
    # absent from the validator's report. Only reportable checks belong here.
    expected = {
        check
        for check in result.statistics.checks
        if check.severity >= requirement_severity
    }
    incomplete = expected - result.executed_checks - result.skipped_checks
    required_incomplete = sorted(
        check.identifier for check in incomplete if check.severity >= models.Severity.REQUIRED
    )
    required_issues = []
    advisory_issues = []
    for issue in result.issues:
        (required_issues if issue.severity >= models.Severity.REQUIRED else advisory_issues).append(
            issue
        )
    if required_issues or required_incomplete:
        details = _unique_issue_messages(required_issues, "REQUIRED")
        skip_details = {
            detail.check.identifier: detail
            for detail in getattr(result, "skipped_check_details", ())
        }
        for identifier in required_incomplete:
            detail = skip_details.get(identifier)
            reason = f" ({detail.message})" if detail and detail.message else ""
            details.append(f'REQUIRED check "{identifier}" did not complete{reason}')
        raise WorkflowError("RO-Crate validation failed:\n" + "\n".join(details))

    if advisory_issues or incomplete:
        advisory_messages = _unique_issue_messages(advisory_issues, "RO-Crate advisory")
        snakemake_logger.warning(
            "RO-Crate is valid (all REQUIRED checks passed); "
            f"{len(advisory_messages)} RECOMMENDED/OPTIONAL finding(s) and "
            f"{len(incomplete)} advisory check(s) not evaluated."
        )
    for check in sorted(incomplete, key=lambda item: item.identifier):
        snakemake_logger.warning(
            f"RO-Crate {check.severity.name} check could not be evaluated: {check.identifier}"
        )
    for message in _unique_issue_messages(advisory_issues, "RO-Crate advisory"):
        snakemake_logger.warning(message)
    snakemake_logger.info("RO-Crate validation succeeded for profile provenance-run-crate-0.6.")
