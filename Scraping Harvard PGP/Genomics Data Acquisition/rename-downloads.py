# rename-downloads.py

import os

#provider = 'CG'
provider = '23andMe'
ext = ".txt"

if provider == 'CG':
	ext = '.tsv'

target_path = '/Users/jerenolsen/Desktop/genome_downloads/{}_final_dir'.format(provider)

for file in os.listdir(target_path):
	if file == '.DS_Store':
		continue

	names = file.strip(ext)	
	names = names.split("_")
	provider = names[1]
	profile = names[2]
	genome = names[3]

	new_name = profile+"_"+provider+"_"+genome+ext

	old_path = os.path.join(target_path,file)
	new_path = os.path.join(target_path,new_name)
	print(f"old_path: {old_path}\nnew_path: {new_path}")
	os.rename(old_path, new_path)

