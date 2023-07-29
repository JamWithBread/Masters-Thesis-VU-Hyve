# run-imputation-all.sh

start_time=$(date +%s)
files=(/home/jeren/pipeline-scripts/output_preprocessing/*)
counter=0
num_files=${#files[@]}
sudo chmod +x /home/jeren/pipeline-scripts/2_Imputation/imputation-pipeline.sh

#for target_file in "${files[@]}"; do
for target_file in ${files[@]}; do
	((counter++))
	echo "#### $counter / $num_files: Running Imputation on $target_file #####"
	#source "/home/jeren/pipeline-scripts/2. Imputation/imputation-pipeline.sh" "$target_file"
	#sudo bash -c "/home/jeren/pipeline-scripts/2_Imputation/imputation-pipeline.sh" "$target_file"
	sudo bash -c "/home/jeren/pipeline-scripts/2_Imputation/imputation-pipeline.sh \"$target_file\""
done

end_time=$(date +%s)
time_duration=$((end_time - start_time))
time_duration_minutes=$((time_duration / 60))
echo "Total Time Duration for run-imputation-all.sh: $time_duration_minutes (minutes)"