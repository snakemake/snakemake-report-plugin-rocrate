### What is recorded

The crate uses RO-Crate 1.1 and conforms to Provenance Run Crate 0.5, Workflow
Run Crate 0.5, Process Run Crate 0.5 and Workflow RO-Crate 1.0. It contains:

- the main `Snakefile` as the crate's main workflow, with one `HowToStep` per
  rule;
- one `CreateAction` per executed job, with its start and end time, its input
  files (`object`), output files (`result`) and shell command;
- a `CreateAction` for the workflow run as a whole, and an `OrganizeAction`
  for Snakemake orchestrating it, linked to the jobs through `ControlAction`s;
- the input and output files, including intermediate files. Files outside the
  working directory are copied under `_EXTERNAL/` in the ZIP;
- the Conda environment files of the jobs, and the packages installed in each
  job's environment with their versions.

External inputs (files consumed by a job but not produced by another job) are
recorded as the workflow run's inputs; final outputs as its outputs.

### Run the report after the workflow

Generate the report from the workflow directory after the run, with the same
targets and software deployment method:

```bash
snakemake --cores 1 --software-deployment-method conda
snakemake --cores 1 --software-deployment-method conda --reporter rocrate
```

Use `--software-deployment-method conda` for the report as well, so that the
plugin can find each job's Conda environment and read the installed package
versions from its `conda-meta` records. Without it, no package versions are
recorded. Packages installed with pip only are not listed in `conda-meta` and
are therefore not recorded.

The Snakemake version recorded is the one used to generate the report, which
may differ from the version that ran the jobs.

### Setting options in a profile

The options are easiest to keep in a
[Snakemake profile](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles),
for example `profiles/rocrate/config.yaml`:

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

and then run:

```bash
snakemake --software-deployment-method conda --reporter rocrate --profile profiles/rocrate --cores 1
```

### People and organizations

Two kinds of person can be recorded, and both are optional:

- the **researcher** (`researcher-name`, `researcher-orcid`) is recorded as the
  crate's author. With `organization-*` options, the researcher is affiliated
  with that organization, and the organization is the crate's publisher;
- the **agent** (`agent-name` together with `agent-orcid`, which must be an
  HTTP(S) ORCID URL) is the person who ran the workflow. It is recorded as the
  `agent` of the workflow run, of each job and of the orchestration action.

The plugin does not record any person or organization unless these options
are given.

### Main tool

Each executed rule is represented by a tool entity that is the instrument of
its jobs. With `main-tool`, the named package (if it is installed in a job's
environment) is listed first among that tool's `softwareRequirements`. Without
`main-tool`, the packages discovered in each job's environment are used
directly as the job's instruments. If packages were discovered but none has
the name given in `main-tool`, the report fails and lists the available
packages.

### Metadata-only mode

With `metadata-only`, the plugin writes only `ro-crate-metadata.json` into the
working directory, next to the `Snakefile` and the workflow outputs, instead of
writing a ZIP. No files are copied: the identifiers in the metadata are paths
relative to the working directory. `filename` is ignored in this mode.

### Validation

The crate is validated against the Provenance Run Crate 0.5 rules of
[roc-validator](https://github.com/crs4/rocrate-validator).
`validation-severity` sets the lowest level to inspect:

- a failed REQUIRED check always makes the report fail;
- failed RECOMMENDED or OPTIONAL checks are logged as warnings;
- checks that could not be evaluated are logged separately, and an
  unevaluated REQUIRED check also makes the report fail.

### Files with several parts

The plugin packages the input and output files that Snakemake declares, and
does not inspect file contents. If a rule produces a dataset made of several
files (for example a mesh with sidecar files), declare each file as an output
so that all of them are included.

### Examples

The [repository](https://github.com/snakemake/snakemake-report-plugin-rocrate/tree/main/examples)
contains two example workflows, a linear-elastic plate with a hole (FEniCS/DOLFINx)
and a Poisson equation solved and written up as a PDF. Each comes with
`profiles/rocrate` and `profiles/rocrate-metadata-only` configurations.
