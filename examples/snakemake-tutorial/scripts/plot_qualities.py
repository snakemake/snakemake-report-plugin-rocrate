"""Plot quality scores from the variant calls in the tutorial workflow."""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from pysam import VariantFile

# Snakemake injects the ``snakemake`` object when it runs this script.
with VariantFile(snakemake.input[0]) as variants:  # noqa: F821
    qualities = [record.qual for record in variants if record.qual is not None]

plt.hist(qualities)
plt.xlabel("Variant quality")
plt.ylabel("Count")
plt.savefig(snakemake.output[0])  # noqa: F821
