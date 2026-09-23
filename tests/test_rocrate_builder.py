from pathlib import Path
from types import SimpleNamespace

import pytest

from snakemake_report_plugin_rocrate import ReportSettings
from snakemake_report_plugin_rocrate.jsonld import as_list
from snakemake_report_plugin_rocrate.models import CrateFile, ProvenanceResult
from snakemake_report_plugin_rocrate.provenance_graph import ProvenanceGraphHelpers
from snakemake_report_plugin_rocrate.rocrate_builder import ProvenanceRunCrateBuilder
from snakemake_report_plugin_rocrate.validator import validate_rocrate


@pytest.mark.parametrize("with_tool", [True, False])
@pytest.mark.parametrize("workflow_first", [True, False])
def test_workflow_boundary_and_tool_parameters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, with_tool: bool, workflow_first: bool
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("Snakefile").write_text("rule all:\n    input: 'final.txt'\n")
    files = ["source.txt", "shared.txt", "middle.txt", "final.txt", "side.txt"]
    for name in files:
        Path(name).write_text(name)

    def action(name: str, inputs: list[str], outputs: list[str]) -> dict:
        return {
            "@id": f"local:{name}",
            "@type": "action",
            "rule": name,
            "has input": [{"@id": f"local:{value}"} for value in inputs],
            "has output": [{"@id": f"local:{value}"} for value in outputs],
        }

    jobs = {
        "first": action("first", ["source.txt", "shared.txt"], ["middle.txt", "side.txt"]),
        "second": action("second", ["middle.txt", "shared.txt"], ["final.txt"]),
    }
    workflow = {"workflow": action("action_workflow_run", [], [])}
    provenance = ProvenanceResult(
        file_nodes={name: {"@id": f"local:{name}", "label": name} for name in files},
        actions=(workflow | jobs) if workflow_first else (jobs | workflow),
        methods={},
        tools={"tool": {"@id": "local:tool", "label": "solver"}} if with_tool else {},
    )
    Path("README.md").write_text("# Workflow\n")
    provenance.supplemental_files.append(
        CrateFile("README.md", "README.md", "README.md", "text/x-markdown")
    )
    builder = ProvenanceRunCrateBuilder(
        dag=SimpleNamespace(workflow=SimpleNamespace(main_snakefile="Snakefile")),
        settings=ReportSettings(workflow_inputs='{"source": "source.txt", "shared": "shared.txt"}'),
    )
    builder.build(provenance)
    graph = {entity.id: entity.properties() for entity in builder.crate.get_entities()}

    def ids(entity_id: str, property_name: str) -> list[str]:
        return [ref["@id"] for ref in as_list(graph[entity_id].get(property_name))]

    def names(entity_id: str, direction: str) -> list[str]:
        return [graph[ref]["name"] for ref in ids(entity_id, direction)]

    assert names("Snakefile", "input") == ["source", "shared"]
    assert names("Snakefile", "output") == [
        "workflow.action_workflow_run.output.1",
        "workflow.action_workflow_run.output.2",
    ]
    assert ids("#action_workflow_run", "object") == ["source.txt", "shared.txt"]
    assert ids("#action_workflow_run", "result") == ["side.txt", "final.txt"]
    tool_id = "#tool" if with_tool else "#snakemake-rule-executor"
    assert names(tool_id, "input") == [
        "first.input.1",
        "first.input.2",
        "second.input.1",
        "second.input.2",
    ]
    assert names(tool_id, "output") == ["first.output.1", "first.output.2", "second.output.1"]
    assert ids("#first", "result") == ["middle.txt", "side.txt"]
    assert ids("#second", "object") == ["middle.txt", "shared.txt"]
    assert not set(ids("Snakefile", "input")) & set(ids(tool_id, "input"))
    for owner in ["Snakefile", tool_id]:
        for direction in ["input", "output"]:
            for parameter_id in ids(owner, direction):
                assert any(parameter_id in ids(file_id, "exampleOfWork") for file_id in files)
    assert provenance.actions["workflow"]["has input"] == []
    assert graph["README.md"]["encodingFormat"] == "text/markdown"
    assert graph["README.md"]["about"] == {"@id": "./"}
    assert graph["Snakefile"]["conformsTo"] == {
        "@id": "https://bioschemas.org/profiles/ComputationalWorkflow/1.0-RELEASE"
    }
    for entity in graph.values():
        if entity.get("@type") == "SoftwareApplication":
            assert not ({"version", "softwareVersion"} <= entity.keys())
    if not with_tool and workflow_first:
        archive_path = tmp_path / "profile-05.zip"
        builder.crate.write_zip(archive_path)
        validate_rocrate(archive_path)


def test_timestamps_use_validator_compatible_milliseconds() -> None:
    assert ProvenanceGraphHelpers()._get_time_str(0) == "1970-01-01T00:00:00.000+00:00"


def test_named_and_positional_slots_across_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from snakemake.iocontainers import Namedlist

    from snakemake_report_plugin_rocrate.provenance_files import FileProvenanceHelpers

    monkeypatch.chdir(tmp_path)
    Path("Snakefile").write_text("")
    helper = FileProvenanceHelpers()
    file_nodes = {}
    actions = {"workflow": {"@id": "local:action_workflow_run", "@type": "action"}}
    for number in (1, 2):
        inputs = Namedlist()
        inputs.extend([f"parameters_{number}.json", "experiment.json"])
        inputs._set_name("parameters", 0)
        outputs = Namedlist()
        outputs.extend([f"summary_{number}.json"])
        outputs._set_name("summary", 0)
        for path in [*inputs, *outputs]:
            Path(path).write_text("")
        helper.dag = SimpleNamespace(
            jobs=[SimpleNamespace(jobid=number, input=inputs, output=outputs)]
        )
        action = {
            "@id": f"local:action_{number}",
            "@type": "action",
            "has input": [],
            "has output": [],
        }
        helper._populate_action_files(
            SimpleNamespace(job=SimpleNamespace(jobid=number), rule="simulate"),
            action,
            file_nodes,
        )
        actions[str(number)] = action

    builder = ProvenanceRunCrateBuilder(
        dag=SimpleNamespace(workflow=SimpleNamespace(main_snakefile="Snakefile")),
        settings=ReportSettings(),
    )
    builder.build(ProvenanceResult(file_nodes=file_nodes, actions=actions, methods={}, tools={}))
    graph = {entity.id: entity.properties() for entity in builder.crate.get_entities()}

    def refs(entity: str, key: str) -> list[str]:
        return [ref["@id"] for ref in as_list(graph[entity].get(key))]

    tool_inputs = refs("#snakemake-rule-executor", "input")
    assert [graph[ref]["name"] for ref in tool_inputs] == ["parameters", "simulate.input.2"]
    assert len(refs("#snakemake-rule-executor", "output")) == 1
    assert refs("Snakefile", "input") == []
    assert len(refs("Snakefile", "output")) == 1
    assert refs("#action_workflow_run", "result") == ["summary_1.json", "summary_2.json"]
    for number in (1, 2):
        assert tool_inputs[0] in refs(f"parameters_{number}.json", "exampleOfWork")
        assert refs(f"#action_{number}", "result") == [f"summary_{number}.json"]
    assert len(refs("experiment.json", "exampleOfWork")) == 1
    assert all(
        graph[ref]["name"] not in file_nodes for ref in tool_inputs + refs("Snakefile", "input")
    )
