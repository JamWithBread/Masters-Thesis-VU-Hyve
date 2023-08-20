#!/bin/bash

merge_dir="$1" # ie: '/Users/jerenolsen/Desktop/All_Tests/merge_files'
ref_assembly="$2" # ie: '/Users/jerenolsen/Desktop/All_Tests/ref_assembly/Homo_sapiens.GRCh37.dna.primary_assembly.fa.fai'
files=("$merge_dir"/*)

mkdir "$merge_dir"/merge_files_reheader

counter=0
num_files=${#files[@]}

for file in "${files[@]}";
do
    fname=$(basename "$file")

    if [[ "$fname" == *".DS_Store"* ]]; then
        echo "Skipping .DS_Store file: $file"
        continue
    fi

    ((counter++))

    echo "$counter / $num_files reheader underway for $fname"
    bcftools reheader --fai "$ref_assembly" "$file" -o "$merge_dir/merge_files_reheader/$fname"
done