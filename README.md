# snakemake-report-plugin-rocrate

> ⚠️ **This plugin is currently under active development and not yet ready for production use.**

Snakemake report plugin to automatically create a [Provenance Run Crate](https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate/) after a workflow run, capturing all metadata about the workflow execution. Provenance Run Crate is the most detailed profile in the [Workflow Run RO-Crate (WRROC)](https://www.researchobject.org/workflow-run-crate/) profile collection, recording provenance at the level of individual tool executions.

## About

RO-Crate is a community effort to establish a lightweight approach to packaging research data with their metadata.
The Workflow Run RO-Crate community is part of the [RO-Crate community](https://www.researchobject.org/ro-crate/community) and develops extensions to capture provenance of computational workflows. 


- [RO-Crate](https://www.researchobject.org/ro-crate/)
- [Provenance Run Crate profile](https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate/)
- [Workflow Run RO-Crate profile family](https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate/)
- [Workflow Run Crate working group](https://www.researchobject.org/workflow-run-crate/)

## Status

This plugin is under active development. Features may be incomplete or subject to change. See the [issue tracker](https://github.com/snakemake/snakemake-report-plugin-rocrate/issues) for known issues and planned features.

## Installation

The plugin is not currently published on PyPI and must be installed from Git.
Install the latest version directly from GitHub:

```bash
python -m pip install \
  "git+https://github.com/snakemake/snakemake-report-plugin-rocrate.git"
```

For development, clone the repository and install it in editable mode:

```bash
git clone https://github.com/snakemake/snakemake-report-plugin-rocrate.git
cd snakemake-report-plugin-rocrate
python -m pip install --editable .
```

## Usage

The reporter collects execution metadata from Snakemake jobs and writes a
Provenance Run Crate 0.5 ZIP using RO-Crate 1.1 and Workflow RO-Crate 1.0. The
reporter validates the finished archive at the selected severity.

## Reporter arguments

All reporter options use the prefix `--report-rocrate-`.

### `filename`

Sets the output filename without the `.zip` suffix. For example,
`--report-rocrate-filename workflow-run` creates `workflow-run.zip`. If this
argument is omitted, the reporter creates `ro-crate.zip`.

### `run-name`

Sets the human-readable name of the workflow run and the root dataset. The
default is `Snakemake Provenance Run`.

### `run-description`

Describes the workflow run, its purpose, or its execution context. The default
is `RO-Crate describing a Snakemake workflow run.`

### `run-license`

Sets the license applying to the crate. It accepts an SPDX identifier such as
`CC-BY-4.0` or a license URL. The default is `CC-BY-4.0`.

### `main-tool`

Selects the primary `SoftwareApplication` by matching the supplied value with
a discovered tool name. The match is case-insensitive. Actions use this tool
as their `instrument`, while all other discovered tools are attached through
`softwareRequirements`. A single discovered tool is selected automatically;
when multiple tools are found, this argument is required.

### `workflow-inputs`

Declares the workflow input interface as a JSON object mapping slot names to
external input file paths, relative to the workflow working directory:

```bash
--report-rocrate-workflow-inputs '{"experiment": "experiment.json", "parameters": "parameters_1.json"}'
```

By default this is `{}`. External files are always recorded on the workflow
run's `object`, but are not automatically declared as workflow parameter slots.
This avoids treating every hardcoded configuration file as a public workflow
input. Rule inputs and outputs still have their own parameters; unnamed rule
slots use positional names. Intermediate files are excluded from the workflow
boundary. Final outputs retain workflow output parameters.

Only external input files can be mapped; invalid or intermediate paths are
rejected. Several slot names may map to the same value.

### `validation-severity`

Sets the minimum official `roc-validator` validation level. Accepted values
are `REQUIRED`, `RECOMMENDED`, and `OPTIONAL`; the default is `REQUIRED`.

### `researcher-orcid`

Sets the ORCID URL or identifier of the person responsible for the workflow
run, for example `https://orcid.org/0000-0002-1825-0097`.

### `researcher-name`

Sets the full name of the person responsible for the workflow run. When an
organization is supplied, the researcher is affiliated with that organization.

### `agent-orcid`

Sets the HTTP(S) ORCID URL of the person responsible for workflow orchestration.
When supplied, `agent-name` must also be provided. The person is linked from the
workflow run, individual tool runs, and `OrganizeAction` through `agent`.

### `agent-name`

Sets the full name of the workflow orchestration agent. It must be supplied
together with `agent-orcid`.

### `organization-ror`

Sets the ROR URL or identifier used as the organization's `@id`, for example
`https://ror.org/04vnq7t77`. The reporter does not contact the ROR API.

### `organization-name`

Sets the human-readable organization name. It is recorded on the organization
entity and can be used even when no ROR identifier is supplied.

### `organization-url`

Sets the organization's website URL. It is recorded directly from the supplied
value without making a network request.

## Running the examples

### Linear-elastic plate with a hole

The repository includes a FEniCS/DOLFINx workflow for a linear-elastic plate
with a hole. From the repository root, navigate to the example and generate the RO-Crate:

```bash
cd examples/linear-elastic-plate-with-hole/fenics-dolfinx

snakemake \
  --software-deployment-method conda \
  --reporter rocrate \
  --report-rocrate-filename "workflow-run" \
  --report-rocrate-run-name "Linear elastic plate with a hole" \
  --report-rocrate-run-license "CC-BY-4.0" \
  --report-rocrate-main-tool "fenics-dolfinx" \
  --report-rocrate-workflow-inputs '{"experiment": "experiment.json", "parameters": "parameters_1.json"}' \
  --report-rocrate-validation-severity "REQUIRED" \
  --report-rocrate-agent-orcid "https://orcid.org/0009-0008-6162-8404" \
  --report-rocrate-agent-name "Mahdi Jafarkhani" \
  --report-rocrate-organization-ror "https://ror.org/04vnq7t77" \
  --report-rocrate-organization-name "University of Stuttgart" \
  --report-rocrate-organization-url "https://www.uni-stuttgart.de/en/" \
  --cores 1
```

The reporter writes `workflow-run.zip` in the example directory.

### Poisson equation

The repository also includes a workflow that solves the Poisson equation,
post-processes the solution, and compiles the results into a PDF. From the
repository root, run:

```bash
cd examples/poisson-equation/snakemake

snakemake paper.pdf \
  --software-deployment-method conda \
  --reporter rocrate \
  --report-rocrate-filename "poisson-equation-workflow-run" \
  --report-rocrate-run-name "Poisson equation" \
  --report-rocrate-run-license "MIT" \
  --report-rocrate-main-tool "gmsh" \
  --report-rocrate-validation-severity "REQUIRED" \
  --report-rocrate-agent-orcid "https://orcid.org/0009-0008-6162-8404" \
  --report-rocrate-agent-name "Mahdi Jafarkhani" \
  --report-rocrate-organization-ror "https://ror.org/04vnq7t77" \
  --report-rocrate-organization-name "University of Stuttgart" \
  --report-rocrate-organization-url "https://www.uni-stuttgart.de/en/" \
  --cores 1 \
  --use-conda
```

The reporter writes `poisson-equation-workflow-run.zip` in the example
directory. See the [example README](examples/poisson-equation/snakemake/README.md)
for more information about the workflow and its configuration.


For usage instructions, see the documentation:

- [Introduction](docs/intro.md)
- [Further information](docs/further.md)

## Profile validation

Profile 0.5 is validated directly with the rules distributed by
`roc-validator`.

Sources:

- [Provenance Run Crate 0.5](https://w3id.org/ro/wfrun/provenance/0.5)
- [Workflow RO-Crate 1.0](https://w3id.org/workflowhub/workflow-ro-crate/1.0)

The reporter does not invent metadata that Snakemake does not supply, such as
workflow authorship or a released workflow version. Software versions
come from the job's own Conda environment when available, otherwise exact YAML
pins; missing versions are explicitly recorded as `not recorded`. Package
registry URLs are derived from environment declarations, with a package-search
URL used when the registry is unknown. Report-generation Snakemake versions
are recorded for the engine/language; historical executions may have used a
different version not retained in Snakemake's run metadata.

## Files and environment definitions

The reporter is format-neutral: it packages the input and output files declared
by Snakemake and does not inspect file contents to discover format-specific
sidecar files. Workflows that produce multi-file datasets should declare every
required component as a Snakemake output so it is captured by the report.

Local Conda YAML files are included in the crate when Snakemake reports them.
