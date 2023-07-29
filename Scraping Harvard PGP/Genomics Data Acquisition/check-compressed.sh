#check_compressed.sh

staged_files='/Users/jerenolsen/Desktop/genome_downloads/final_dir'

for file in "$staged_files"/*; do

	file_type=$(file -b "$file")
	#echo "$file"

	# Check if the file type indicates compression
	if [[ $file_type == *"compress"* ]]; then
	  echo "$file is compressed."
	fi

done