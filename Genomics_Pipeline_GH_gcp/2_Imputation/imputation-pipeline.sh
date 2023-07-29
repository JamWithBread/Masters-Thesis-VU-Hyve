# imputation-pipeline.sh

# Example: ./imputation-pipeline.sh input/hu1AF4ED_23andMe_processed.vcf.gz

### Run Beagle5.4 to impute genotypes for each chromosome in target vcf ###

#TODO: None


start_time=$(date +%s)

target_file=$1 #"input/hu1AF4ED_23andMe_processed.vcf.gz"
reference_panels_dir="/home/jeren/pipeline-scripts/resources/1000G_brefs"

# Get list of reference panels
files=("$reference_panels_dir"/*)

mkdir /home/jeren/pipeline-scripts/temp_imputation
mkdir /home/jeren/pipeline-scripts/temp_imputation/imputed_out
mkdir /home/jeren/pipeline-scripts/output_imputation

echo "imputation-pipeline.sh running with target_file: $target_file"

cp "$target_file" "/home/jeren/pipeline-scripts/temp_imputation/temp_uncompressed.vcf"
uncompressed_gt_vcf="/home/jeren/pipeline-scripts/temp_imputation/temp_uncompressed.vcf"

for ref_file in "${files[@]}"; do

	fname=$(basename "$ref_file")
	echo " "
	echo "Imputing against $fname"
	prefix="${fname#chr}"
	chr_num="${prefix%%.*}"
	chr=$(echo "$fname" | cut -d. -f1)

	#create temp subfile of target vcf file including only the appropriate chr # 
	grep -E "^#|(^${chr_num}\\b)" "$uncompressed_gt_vcf" > /home/jeren/pipeline-scripts/temp_imputation/chr${chr_num}_temp.vcf
	
	#run imputation on covered positions in target vcf file for specific chromosome against corresponding chromosome from reference panel
	java -jar "/home/jeren/pipeline-scripts/2_Imputation/beagle.22Jul22.46e.jar" nthreads=8 seed=-99999 ref=$ref_file gt=/home/jeren/pipeline-scripts/temp_imputation/chr${chr_num}_temp.vcf out="/home/jeren/pipeline-scripts/temp_imputation/imputed_out/${chr}_imputed"

	gunzip "/home/jeren/pipeline-scripts/temp_imputation/imputed_out/${chr}_imputed.vcf.gz"

	rm /home/jeren/pipeline-scripts/temp_imputation/chr${chr_num}_temp.vcf


done

#rm temp/*
rm -f "$uncompressed_gt_vcf"

### Combine all imputed .vcf files into one vcf with all chromosomes ###

#Get list of imputed vcf files
vcf_files_list=()
for file_path in /home/jeren/pipeline-scripts/temp_imputation/imputed_out/*.vcf; do
    file_name=$(basename "$file_path")
    vcf_files_list+=("/home/jeren/pipeline-scripts/temp_imputation/imputed_out/$file_name")
done

# Initialize new file and add header
chr1_file="${vcf_files_list[1]}"
fname=$(basename "$target_file")
output_name=$(echo $fname | cut -d'_' -f1,2 | tr ' ' '_')

grep '^#' $chr1_file > /home/jeren/pipeline-scripts/output_imputation/"${output_name}_imputed.vcf"

#Populate new file with each imputed chromosome
for vcf_file in "${vcf_files_list[@]}"; do
	echo "Merging $vcf_file > /home/jeren/pipeline-scripts/output_imputation/${output_name}_imputed.vcf"
	# Append the contents of each VCF file to merged file
	grep -v '^#' "$vcf_file" >> /home/jeren/pipeline-scripts/output_imputation/"${output_name}_imputed.vcf"
done

#clear imputed chromosomes from imputed_out
rm /home/jeren/pipeline-scripts/temp_imputation/imputed_out/*

#delete all files but not folders in temp dir
find /home/jeren/pipeline-scripts/temp_imputation -type f -delete

end_time=$(date +%s)
time_duration=$((end_time - start_time))
echo "Total Time Duration (seconds): $time_duration"


