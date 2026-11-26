from types import SimpleNamespace

import pytest
from rocrate_validator.models import Severity
from snakemake.exceptions import WorkflowError

from snakemake_report_plugin_rocrate.validator import validate_rocrate


def test_validation_reports_issues_before_incomplete_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Check:
        identifier = "ro-crate-1.2_4.3"
        severity = Severity.REQUIRED
        overridden = False
        deactivated = False

    check = Check()
    requirement = SimpleNamespace(get_checks=lambda: [check])
    profile = SimpleNamespace(get_requirements=lambda severity: [requirement])
    issue = SimpleNamespace(
        severity=Severity.REQUIRED,
        check=SimpleNamespace(identifier="ro-crate-1.2_4.2"),
        message="The crate metadata is missing a required property.",
    )
    same_message_other_check = SimpleNamespace(
        severity=Severity.REQUIRED,
        check=SimpleNamespace(identifier="ro-crate-1.2_4.1"),
        message=issue.message,
    )
    result = SimpleNamespace(
        context=SimpleNamespace(profiles=[profile]),
        statistics=SimpleNamespace(checks={check}),
        executed_checks=set(),
        skipped_checks=set(),
        skipped_check_details=[SimpleNamespace(check=check, message="dependency did not pass")],
        issues=[issue, issue, same_message_other_check],
    )
    monkeypatch.setattr(
        "snakemake_report_plugin_rocrate.validator.services.validate",
        lambda settings: result,
    )

    with pytest.raises(WorkflowError) as error:
        validate_rocrate("crate.zip")

    message = str(error.value)
    assert 'REQUIRED checks "ro-crate-1.2_4.2", "ro-crate-1.2_4.1"' in message
    assert message.count("The crate metadata is missing a required property.") == 1
    assert 'REQUIRED check "ro-crate-1.2_4.3" did not complete (dependency did not pass)' in message


def test_hidden_profile_check_does_not_invalidate_crate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HiddenCheck:
        identifier = "ro-crate-1.2_4.3"
        severity = Severity.REQUIRED
        overridden = False
        deactivated = False

    hidden = HiddenCheck()
    requirement = SimpleNamespace(get_checks=lambda: [hidden])
    profile = SimpleNamespace(get_requirements=lambda severity: [requirement])
    result = SimpleNamespace(
        context=SimpleNamespace(profiles=[profile]),
        statistics=SimpleNamespace(checks=set()),
        executed_checks=set(),
        skipped_checks=set(),
        issues=[],
    )
    monkeypatch.setattr(
        "snakemake_report_plugin_rocrate.validator.services.validate",
        lambda settings: result,
    )

    validate_rocrate("crate.zip")
