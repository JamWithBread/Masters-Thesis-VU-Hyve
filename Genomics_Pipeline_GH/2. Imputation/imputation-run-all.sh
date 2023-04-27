# run-imputation-all.sh

start_time=$(date +%s)
files=("input"/*)
counter=0
num_files=${#files[@]}

for target_file in "${files[@]}"; do
	((counter++))
	echo "#### $counter / $num_files: Running Imputation on $target_file #####"
	./imputation-pipeline.sh "$target_file"
done

end_time=$(date +%s)
time_duration=$((end_time - start_time))
time_duration_minutes=$((time_duration / 60))
echo "Total Time Duration for run-imputation-all.sh: $time_duration_minutes (minutes)"