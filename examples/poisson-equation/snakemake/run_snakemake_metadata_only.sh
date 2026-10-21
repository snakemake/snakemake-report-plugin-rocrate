snakemake paper.pdf \
  --software-deployment-method conda \
  --reporter rocrate \
  --profile profiles/rocrate-metadata-only \
  --cores 1 \
  --use-conda
