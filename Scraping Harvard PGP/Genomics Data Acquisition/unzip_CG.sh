
#echo "Copying CG_genome_downloads ..."
#cp -r "$(pwd)/CG_genome_downloads" "$(pwd)/CG_genome_downloads_copy"
base_dir="$(pwd)"

rm -rf "$base_dir/CG_final_dir"

staged_files="$(pwd)/CG_genome_downloads_copy"
temp_dir="$(pwd)/CG_genome_downloads_copy/temp"
unzipped_dir="$(pwd)/CG_genome_downloads_copy/unzipped"

mkdir "$temp_dir"
mkdir "$unzipped_dir"

for file in "$staged_files"/*; do
	if [[ -f "$file" ]]; then
		b_name=$(basename "$file")
		new_filename="${b_name%.*}"

		echo "Unzipping $b_name -> $new_filename"

		mv "$file" "$temp_dir"
		cd "$temp_dir"
		bzip2 -d "$b_name" #2>/dev/null


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

mv "$unzipped_dir" "$base_dir/CG_final_dir"
rm -rf "$staged_files"



