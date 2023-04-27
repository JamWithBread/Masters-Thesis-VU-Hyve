# imputation-pipeline.sh

# Example: ./imputation-pipeline.sh input/hu1AF4ED_23andMe_processed.vcf.gz

### Run Beagle5.4 to impute genotypes for each chromosome in target vcf ###

#TODO: None


start_time=$(date +%s)

target_file=$1 #"input/hu1AF4ED_23andMe_processed.vcf.gz"
reference_panels_dir="1000G_reference_panels/1000G_brefs"

# Get list of reference panels
files=("$reference_panels_dir"/*)

#gunzip -c "$target_file" > "$(pwd)/temp/temp_uncompressed.vcf"
cp "$target_file" "$(pwd)/temp/temp_uncompressed.vcf"
uncompressed_gt_vcf="$(pwd)/temp/temp_uncompressed.vcf"

for ref_file in "${files[@]}"; do

	fname=$(basename "$ref_file")
	echo " "
	echo "Imputing against $fname"
	prefix="${fname#chr}"
	chr_num="${prefix%%.*}"
	chr=$(echo "$fname" | cut -d. -f1)

	#create temp subfile of target vcf file including only the appropriate chr # 
	grep -E "^#|(^${chr_num}\\b)" "$uncompressed_gt_vcf" > temp/chr${chr_num}_temp.vcf
	
	#run imputation on covered positions in target vcf file for specific chromosome against corresponding chromosome from reference panel
	java -jar beagle.22Jul22.46e.jar ref=$ref_file gt=temp/chr${chr_num}_temp.vcf out="imputed_out/${chr}_imputed"

	gunzip "imputed_out/${chr}_imputed.vcf.gz"

	rm temp/chr${chr_num}_temp.vcf

done

rm temp/*
rm -f "$uncompressed_gt_vcf"

### Combine all imputed .vcf files into one vcf with all chromosomes ###

#Get list of imputed vcf files
vcf_files_list=()
for file_path in imputed_out/*.vcf; do
    file_name=$(basename "$file_path")
    vcf_files_list+=("imputed_out/$file_name")
done

# Initialize new file and add header
chr1_file="${vcf_files_list[1]}"
fname=$(basename "$target_file")
output_name=$(echo $fname | cut -d'_' -f1,2 | tr ' ' '_')

grep '^#' $chr1_file > final_output/"${output_name}_imputed.vcf"

#Populate new file with each imputed chromosome
for vcf_file in "${vcf_files_list[@]}"; do
	echo "Merging $vcf_file > final_output/${output_name}_imputed.vcf"
	# Append the contents of each VCF file to merged file
	grep -v '^#' "$vcf_file" >> final_output/"${output_name}_imputed.vcf"
done

#clear imputed chromosomes from imputed_out
rm imputed_out/*

end_time=$(date +%s)
time_duration=$((end_time - start_time))
echo "Total Time Duration (seconds): $time_duration"


