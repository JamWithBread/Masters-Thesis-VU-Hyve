#Run plink on 23andMe-like .txt file

preprocessed_dir="/Users/jerenolsen/desktop/Genomics Pipeline/1. Preprocessing/output" 

# Get list of preprocessed genome text files
#files=$(ls "$preprocessed_dir")
files=("$preprocessed_dir"/*)

for genome_text_file in "${files[@]}"; do

      fname=$(basename "$genome_text_file")
      echo "Converting to vcf: $fname"

      profile_provider=$(echo "$fname" | cut -d'_' -f1,2)

      # plink --23file $genome_text_file \
      #   --out "output_vcf/${profile_provider}" \
      #   --recode vcf \
      #   --set-missing-var-ids @:# \
      #   --snps-only just-acgt

      plink --file $genome_text_file\
            --recode vcf \
            --set-missing-var-ids @:# \
            --snps-only just-acgt \
            --out "output_vcf/${profile_provider}"

      # plink --file $genome_text_file \
      #       --make-bed \
      #       --out "temp/${profile_provider}"

      # plink --bfile "temp/${profile_provider}" \
      #       --recode vcf \
      #       --set-missing-var-ids @:# \
      #       --snps-only just-acgt \
      #       --out "output_vcf/${profile_provider}"

      break


done 
# plink --file huF80F84_CGI_preprocessed.txt \
#       --allow-no-sex \
#       --recode \
#       --out huF80F84_CGI_preprocessed

# plink --file huF80F84_CGI_preprocessed.txt \
#       --make-bed \
#       --allow-no-sex \
#       --geno 0 \
#       --maf 0.1 \
#       --hwe 0 \
#       --out huF80F84_CGI


# plink --file huF80F84_CGI \
#       --recode vcf \
#       --out huF80F84_CGI_vcf_test


#Remove duplicates

# input_file="huF80F84_CGI_vcf_test.vcf"  # Replace with the path to your input VCF file
# output_file="$(pwd)/huF80F84_CGI_vcf_test_noduplicates.vcf"  # Replace with the desired output file name

# grep "^#" $input_file > $output_file  # Copy header lines to output file

# grep -v "^#" $input_file | sort -k1,1 -k2,2n | awk '!a[$1" "$2]++' >> $output_file  # Sort and remove duplicates based on positions

# echo "Duplicate variants removed. Output file: $output_file"