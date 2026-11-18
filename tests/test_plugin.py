import pytest
import snakemake.common.tests
from snakemake.exceptions import WorkflowError
from snakemake_interface_report_plugins.settings import ReportSettingsBase

from snakemake_report_plugin_rocrate import Reporter, ReportSettings


def test_report_without_completed_job_metadata_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reporter = object.__new__(Reporter)
    reporter.jobs = []
    reporter.settings = ReportSettings()

    def unexpected_builder(**kwargs: object) -> None:
        pytest.fail("The reporter must exit before creating a provenance workspace")

    monkeypatch.setattr("snakemake_report_plugin_rocrate.ProvenanceBuilder", unexpected_builder)
    with pytest.raises(WorkflowError, match="Run the requested workflow targets first"):
        reporter.render()


# Check out the base classes found here for all possible options and methods:
# https://github.com/snakemake/snakemake/blob/main/src/snakemake/common/tests/__init__.py
@pytest.mark.skip(reason="Currently not working")
class TestWorkflowsBase(snakemake.common.tests.TestReportBase):  # type: ignore[misc]
    __test__ = True

    def get_reporter(self) -> str:
        return "rocrate"

    def get_report_settings(self) -> ReportSettingsBase | None:
        # instantiate ReportSettings of this plugin as appropriate
        return None
