This report plugin packages a finished Snakemake workflow run as a
[Provenance Run Crate](https://w3id.org/ro/wfrun/provenance/0.5), the most
detailed profile of the [Workflow Run RO-Crate](https://www.researchobject.org/workflow-run-crate/)
family. The crate records which rules ran, when, with which input and output
files and with which software, in a machine-readable
[RO-Crate](https://www.researchobject.org/ro-crate/) that can be archived,
shared or deposited alongside the results.

The plugin writes either a ZIP archive containing the workflow files, the
data and `ro-crate-metadata.json`, or only `ro-crate-metadata.json` next to
the workflow files. The crate is validated with
[roc-validator](https://github.com/crs4/rocrate-validator) before the report
completes.
