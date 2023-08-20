#!/bin/bash

cohort_bfile="$1"
pruning_outpath="$2"
eigenvec_outpath="$3"
pruned_file=$pruning_outpath
prune_in="$pruned_file".prune.in

# First, we need to perform prunning
plink \
    --bfile $cohort_bfile \
    --indep-pairwise 200 50 0.25 \
    --out $pruning_outpath
# Then we calculate the first 6 PCs

plink \
    --bfile $pruned_file \
    --extract $prune_in \
    --pca 6 \
    --out $eigenvec_outpath