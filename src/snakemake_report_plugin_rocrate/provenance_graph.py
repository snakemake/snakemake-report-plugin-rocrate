"""Graph-level provenance helpers."""

from datetime import datetime


class ProvenanceGraphHelpers:
    """Helpers that create graph-level nodes and relationships."""

    def _add_workflow_run_action(self, sorted_jobs) -> None:
        """Create the workflow-level action spanning all executed jobs."""
        self.state.workflow_run_action_id = "local:action_workflow_run"
        earliest_start = min(item.starttime for item in sorted_jobs)
        latest_end = max(item.endtime for item in sorted_jobs)
        workflow_action_node = {
            "@id": self.state.workflow_run_action_id,
            "@type": "action",
            "label": "workflow run",
            "start time": self._get_time_str(earliest_start),
            "end time": self._get_time_str(latest_end),
            "has input": [],
            "has output": [],
        }
        self.state.actions[self.state.workflow_run_action_id] = workflow_action_node

    def _add_precedes_relations(self) -> None:
        """Infer action ordering from shared output and input files."""
        actions = list(self.state.actions.values())
        for source in actions:
            output_ids = {
                ref.get("@id")
                for ref in source.get("has output", [])
                if isinstance(ref, dict)
            }
            if not output_ids:
                continue
            for target in actions:
                if source is target:
                    continue
                input_ids = {
                    ref.get("@id")
                    for ref in target.get("has input", [])
                    if isinstance(ref, dict)
                }
                if output_ids & input_ids:
                    source.setdefault("precedes", []).append({"@id": target["@id"]})

    def _get_time_str(self, timestamp) -> str:
        """Convert a Unix timestamp into a local datetime string."""
        try:
            return f"{datetime.fromtimestamp(timestamp)}"
        except Exception:
            return ""
