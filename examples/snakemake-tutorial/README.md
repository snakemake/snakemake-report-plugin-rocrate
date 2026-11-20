# Snakemake tutorial RO-Crate example

This example adapts the read mapping and variant calling workflow from the
[official Snakemake basics tutorial](https://snakemake.readthedocs.io/en/stable/tutorial/basics.html).
It maps reads from samples A and B, sorts and indexes their alignments, calls
variants, and plots variant quality scores. The input data comes from the
[official tutorial data repository](https://github.com/snakemake/snakemake-tutorial-data).

Run these commands from this directory:

```bash
curl -L https://api.github.com/repos/snakemake/snakemake-tutorial-data/tarball \
  -o snakemake-tutorial-data.tar.gz
tar -xf snakemake-tutorial-data.tar.gz --strip 1 '*/data'

mamba env create -n snakemake-rocrate-tutorial -f environment.yaml
eval "$(mamba shell hook --shell zsh)"  
mamba activate snakemake-rocrate-tutorial

snakemake --cores 2
snakemake --cores 2 --reporter rocrate --profile profiles/rocrate 
```

The reporter writes `workflow-run.zip` in this directory. The archive contains the
workflow, input and output files, and run provenance. The generated files and
downloaded data are ignored by Git.
