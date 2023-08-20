#!/bin/bash

prsice_R_script="$1" #PRSice_mac/PRSice.R
prsice_binary="$2" # PRSice_mac/PRSice_mac
GWAS_path="$3" # UKB_GWAS_SumStats_GERD_processed.txt
target_data="$4" # .../plink_output/cohort_plinked{.fam / .bed / .bim}
phenotype_file="$5" # cohort_phenotypes.pheno
covariate="$6" # cohort_covariates.covariate
outpath="$7" # .../PRS_output/cohort_prs

echo "outpath: $outpath"

Rscript $prsice_R_script \
    --prsice $prsice_binary \
    --thread 8 \
    --base $GWAS_path \
    --target $target_data \
    --binary-target T \
    --quantile 20 \
    --a1 alt \
    --a2 ref \
    --pheno $phenotype_file \
    --cov $covariate \
    --stat BETA \
    --chr-id c:l-ab \
    --pvalue P \
    --out $outpath


