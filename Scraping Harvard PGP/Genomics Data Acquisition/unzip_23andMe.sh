#!/bin/sh

echo "Copying 23andMe_genome_downloads"
cp -r "$(pwd)/23andMe_genome_downloads" "$(pwd)/23andMe_genome_downloads_copy"
base_dir="$(pwd)"

rm -rf "$base_dir/final_dir"

staged_files="$(pwd)/23andMe_genome_downloads_copy"
temp_dir="$(pwd)/23andMe_genome_downloads_copy/temp"
unzipped_dir="$(pwd)/23andMe_genome_downloads_copy/unzipped"

mkdir "$temp_dir"
mkdir "$unzipped_dir"

for file in "$staged_files"/*; do
	if [[ -f "$file" ]]; then
		b_name=$(basename "$file")
		new_filename="${b_name%.*}.txt"

		file_type=$(file -b "$file")
		echo "File $file is type: $file_type"

		mv "$file" "$temp_dir"
		cd "$temp_dir"


		if [[ $file_type == *"gzip"* ]]; then
			gzip_file="${b_name%.*}.gz"
			mv "$b_name" "$temp_dir/$gzip_file"
			echo "Decompressing gzipped file: $gzip_file"
			gunzip "$gzip_file"

		elif [[ $file_type == *"bzip2"* ]]; then
			bzip_file="${b_name%.*}.bz2"
			mv "$b_name" "$temp_dir/$bzip_file"
			echo "Decompressing bzip2 file: $bzip_file"
			bunzip2 "$bzip_file"

		elif [[ $file_type == *"compress"* ]]; then
			echo "Decompressing $file"
	  		unzip "$b_name" #2>/dev/null
		fi



		for file in "$temp_dir"/*; do
			if [[ -f "$file" ]]; then
				mv "$file" "./$new_filename"
			fi
		done

		mv "$new_filename" "$unzipped_dir/$new_filename"
		rm -rf "$/temp_dir"/*
		cd "$base_dir"


	fi
done

mv "$unzipped_dir" "$base_dir/final_dir"
rm -rf "$staged_files"






