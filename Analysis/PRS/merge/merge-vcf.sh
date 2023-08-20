#!/bin/bash

rm -f file.list
reheadered_dir="$1" 
output_path="$2" #ie: /Users/jerenolsen/Desktop/All_Tests/PRSice_Testing/Test_Run/merge/merged_cohort_files.vcf.gz

# Set the batch size to control the number of files merged at once
batch_size=50
batch_counter=0
global_counter=0

for F in "$reheadered_dir"/*; do
    ((batch_counter++))
    ((global_counter++))
    echo "$global_counter - Indexing $(basename "$F")"

    bcftools sort -O b -o ${F}.bcf $F
    bcftools index ${F}.bcf
    echo "${F}.bcf" >> file.list

    # Merge the files in batches of $batch_size
    if [ $batch_counter -ge $batch_size ]; then
        echo "Merging batch of $batch_size files ..."

        batch_number=$(( (global_counter - 1) / batch_size + 1 ))

        bcftools merge --file-list file.list --force-samples -O z -o ${output_path%.*}_batch${batch_number}.vcf.gz

        # Reset counter and file list for the next batch
        batch_counter=0
        rm -f file.list
    fi
done


# Merge any remaining files
if [ $batch_counter -gt 0 ]; then
    echo "Merging remaining files ..."
    batch_number=$(( (global_counter - 1) / batch_size + 1 ))
    bcftools merge --file-list file.list --force-samples -O z -o ${output_path%.*}_batch${batch_number}.vcf.gz
    rm -f file.list
fi

#gunzip -f ${output_path%.*}_batch*.vcf.gz

# Merge all the batches into a single file
echo "Merging all batches ..."
for F in ${output_path%.*}_batch*.vcf.gz; do

      echo "$ - Indexing batch file $(basename "$F")"

      bcftools sort -O b -o ${F}.bcf $F
      bcftools index ${F}.bcf
      echo "${F}.bcf" >> file.list
done

echo " - Merging all batch files - "
bcftools merge --file-list file.list -O z -o $output_path
#bcftools merge ${output_path%.*}_batch*.vcf.gz -O z -o $output_path

# Clean up intermediate batch files
rm -f ${output_path%.*}_batch*.vcf.gz
rm -f ${output_path%.*}*.csi
rm -f ${output_path%.*}*.bcf

