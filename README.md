# snakemake-report-plugin-rocrate

Snakemake report plugin to automatically create a [Provenance Run Crate](https://w3id.org/ro/wfrun/provenance/0.5) after a workflow run, capturing metadata about the workflow execution. Provenance Run Crate is the most detailed profile in the [Workflow Run RO-Crate (WRROC)](https://www.researchobject.org/workflow-run-crate/) profile collection, recording provenance at the level of individual tool executions.

## About RO-Crate and WRROC

[RO-Crate](https://www.researchobject.org/ro-crate/) is a community effort to establish a lightweight approach for packaging research data with their metadata in a machine-readable format.
It helps make research output findable, accessible, interoperable, and reusable (FAIR).
The Workflow Run RO-Crate community is part of the [RO-Crate community](https://www.researchobject.org/ro-crate/community) and develops [profiles](https://www.researchobject.org/ro-crate/profiles) to capture the provenance of computational workflow executions.

## Status

This plugin is under active development. Features may be incomplete or subject to change. See the [issue tracker](https://github.com/snakemake/snakemake-report-plugin-rocrate/issues) for known issues and planned features.

## Installation

Install the plugin from PyPI:

```bash
pip install snakemake-report-plugin-rocrate
```

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
reporter [validates](https://github.com/crs4/rocrate-validator) the finished
archive at the selected severity.

## Reporter arguments

All reporter options use the prefix `--report-rocrate-`.

### `filename`

Sets the output filename without the `.zip` suffix. For example,
`--report-rocrate-filename workflow-run` creates `workflow-run.zip`. If this
argument is omitted, the reporter creates `ro-crate.zip`. This argument is
ignored in metadata-only mode.

### `metadata-only`

Writes only the `ro-crate-metadata.json` descriptor directly into the workflow
working directory (the current working directory, i.e. next to the Snakefile and
the workflow outputs) instead of packaging the crate as a ZIP archive. No file is
copied: the descriptor's relative paths resolve against the real workflow files
in place, and files that live outside the working directory are referenced by
their path relative to it. Because the crate is not self-contained, profile
validation is skipped. For example, running the FEniCS/DOLFINx example with
`--report-rocrate-metadata-only` writes `ro-crate-metadata.json` alongside its
`Snakefile`.

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

Selects a preferred package by name and lists it first among a rule tool's
`softwareRequirements` when that package is present in the job environment.
The match is case-insensitive. Each executed rule has its own tool entity as the
`CreateAction.instrument`; packages from that job's environment are attached to
the rule tool through `softwareRequirements`. The rule tool links to its packaged
Conda environment YAML through `subjectOf` when one is available. This option
is optional. When omitted, discovered packages from each job environment are
listed directly as that job's `CreateAction.instrument` and as tools in the
workflow's `hasPart`. If no package metadata is available, the rule tool remains
the instrument.

### Workflow inputs

The reporter finds external inputs from the executed jobs: files
consumed by a job and not produced by another job. It uses Snakemake's named
input slot when available, or the file name otherwise. Repeated names receive
numeric suffixes. These files are recorded on the workflow run's `object` and
as workflow input parameters. Intermediate files are excluded from the workflow
boundary. Final outputs retain workflow output parameters.

### `validation-severity`

Sets the lowest `roc-validator` level to inspect. Accepted values are
`REQUIRED`, `RECOMMENDED`, and `OPTIONAL`; the default is `REQUIRED`.
Missing REQUIRED checks make the report fail at every level. Missing
RECOMMENDED or OPTIONAL checks are logged as advisory findings while the
crate is reported as satisfying the required checks. Checks that could not
be evaluated are reported separately; an unevaluated REQUIRED check still
makes the report fail.

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
  --profile profiles/rocrate \
  --cores 1
```

The reporter writes `workflow-run.zip` in the example directory.

Note that the above command leverages Snakemake profiles to offload
plugin-specific parameter settings to `profiles/rocrate/config.yaml`:

```yaml
report-rocrate-filename: "workflow-run"
report-rocrate-run-name: "Linear elastic plate with a hole"
report-rocrate-run-license: "CC-BY-4.0"
report-rocrate-main-tool: "fenics-dolfinx"
report-rocrate-validation-severity: "REQUIRED"
report-rocrate-agent-orcid: "https://orcid.org/0009-0008-6162-8404"
report-rocrate-agent-name: "Mahdi Jafarkhani"
report-rocrate-organization-ror: "https://ror.org/04vnq7t77"
report-rocrate-organization-name: "University of Stuttgart"
report-rocrate-organization-url: "https://www.uni-stuttgart.de/en/"
```

### Poisson equation

The repository also includes a workflow that solves the Poisson equation,
post-processes the solution, and compiles the results into a PDF. From the
repository root, run:

```bash
cd examples/poisson-equation/snakemake

snakemake paper.pdf \
  --software-deployment-method conda \
  --reporter rocrate \
  --profile profiles/rocrate \
  --cores 1 \
  --use-conda
```

The reporter writes `poisson-equation-workflow-run.zip` in the example
directory. See the [example README]((https://github.com/snakemake/snakemake-report-plugin-rocrate/blob/main/examples/poisson-equation/snakemake/README.md)
for more information about the workflow and its configuration.

## Profile validation

Profile 0.5 is validated directly with the rules distributed by
`roc-validator`.

Sources:

- [Provenance Run Crate 0.5](https://w3id.org/ro/wfrun/provenance/0.5)
- [Workflow RO-Crate 1.0](https://w3id.org/workflowhub/workflow-ro-crate/1.0)

The reporter does not invent metadata that Snakemake does not supply, such as
workflow authorship or a released workflow version. For jobs with a materialized
Conda environment, it reads installed package names and versions from that job's
`conda-meta/*.json` records. This includes packages whose versions were not pinned
in the environment YAML, such as Python, and transitive dependencies. Each version
of a package gets a separate tool entity when jobs used different environments.
If the Conda prefix is unavailable, the reporter makes no installed-package claim.
Run the report with `--software-deployment-method conda` so Snakemake resolves the
job environments. Package URLs come from Conda's installed-package records when
present, with a package-search URL used when no URL is recorded. Packages
installed only through pip are not listed in Conda's `conda-meta` records.
Report-generation Snakemake versions are recorded for the engine/language;
historical executions may have used a
different version not retained in Snakemake's run metadata.

## Files and environment definitions

The reporter is format-neutral: it packages the input and output files declared
by Snakemake and does not inspect file contents to discover format-specific
sidecar files. Workflows that produce multi-file datasets should declare every
required component as a Snakemake output so it is captured by the report.

Local Conda YAML files are included in the crate when Snakemake reports them.

## Contributing to this repository

Please see [CONTRIBUTING.md](https://github.com/snakemake/snakemake-report-plugin-rocrate/blob/main/CONTRIBUTING.md).
