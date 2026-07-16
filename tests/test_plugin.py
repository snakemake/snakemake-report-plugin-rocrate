import pytest
import snakemake.common.tests
from snakemake_interface_report_plugins.settings import ReportSettingsBase


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
