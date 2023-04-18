#!/bin/bash

input_file="23andMe_vcf_test.vcf"  # Replace with the path to your input VCF file
output_file="$(pwd)/23andMe_vcf_test.vcf_noduplicates.vcf"  # Replace with the desired output file name

grep "^#" $input_file > $output_file  # Copy header lines to output file

grep -v "^#" $input_file | sort -k1,1 -k2,2n | awk '!a[$1" "$2]++' >> $output_file  # Sort and remove duplicates based on positions

echo "Duplicate variants removed. Output file: $output_file"