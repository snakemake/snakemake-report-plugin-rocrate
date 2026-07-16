import os
from collections.abc import Generator
from pathlib import Path

import pytest

from snakemake_report_plugin_rocrate.provenance_files import FileProvenanceHelpers


class MockFileProvenanceHelpers(FileProvenanceHelpers):
    """Minimal mock to instantiate the provenance file helper class."""

    def __init__(self) -> None:
        self.external_directory_name = "_EXTERNAL"


@pytest.fixture
def setup_readonly_mock_environment(tmp_path: Path) -> Generator[str]:
    """Fixture to simulate an isolated local project directory and a central read-only repository
    (emulating HPC distributed file systems)."""
    # Local workspace folder representing the user's project
    project_dir = tmp_path / "snakemake-project"
    project_dir.mkdir()

    # External folder simulating the system-level software repository
    central_ro_repo_dir = tmp_path / "central-ro-repo"
    central_ro_repo_dir.mkdir()

    # Create a dummy read-only file within the external repository folder
    mock_ro_file = central_ro_repo_dir / "file.out"
    mock_ro_file.write_text("dummy-text-data-stream-for-provenance-testing")
    mock_ro_file.chmod(0o444)

    # Cache the old Current Working Directory (CWD) and switch context
    # to the local project root
    old_cwd = Path.cwd()
    os.chdir(project_dir)

    yield str(mock_ro_file)

    # Clean up and restore the initial terminal context after tests complete
    os.chdir(old_cwd)


def test_ingestion_is_idempotent_on_readonly_filesystems(
    setup_readonly_mock_environment: str,
) -> None:
    """Verify that ingesting external workflow files is idempotent when dealing
    with read-only target states (common in HPC environments).

    This test ensures the plugin does not crash when a file acts both as an Output
    and an Input in the Snakemake DAG.
    """
    mock_ro_file_path = setup_readonly_mock_environment
    helper = MockFileProvenanceHelpers()

    # First ingestion attempt: Copy the read-only file into the local workspace
    try:
        target_path_1 = helper._copy_external_relative_files(mock_ro_file_path)
    except PermissionError as e:
        pytest.fail(f"First ingestion attempt failed unexpectedly: {e}")

    assert Path(target_path_1).exists(), (
        "The target file must be copied successfully during the first ingestion."
    )

    # Second ingestion attempt: Re-evaluate the exact same file (Idempotency Check)
    try:
        target_path_2 = helper._copy_external_relative_files(mock_ro_file_path)

        assert target_path_1 == target_path_2, (
            "The helper must return the identical structural identifier path for metadata indexing."
        )
    except PermissionError:
        pytest.fail(
            "CRITICAL BUG DETECTED: The plugin crashed with a PermissionError "
            "when attempting to overwrite a read-only workflow file descriptor."
        )
