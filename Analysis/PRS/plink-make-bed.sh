#!/bin/bash

merged_cohort="$1" 
outpath="$2"

echo "Merged cohort file: $merged_cohort"
echo "outpath: $outpath"

# echo "Unzipping merged .vcf.gz file"
# gunzip $merged_cohort

plink --list-duplicate-vars suppress-first \
  --make-bed \
  --out $outpath \
  --vcf "$merged_cohort"