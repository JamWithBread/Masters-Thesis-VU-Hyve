import os 
import pandas as pd


def get_filepaths(cg_dir):
	files = os.listdir(cg_dir)
	files = [f for f in files if '.DS_Store' not in f]
	files = [os.path.join(cg_dir, f) for f in files]

	return files

def read_header(filepath):
	header_lines = []
	columns = None
	with open(filepath, 'r') as f:
		for line in f:
			if line[0] == '#':
				header_lines.append(line.strip('\n').split('\t'))
			elif '>locus' in line:
				columns = line.strip('\n').split('\t')
				break

	return header_lines, columns

def init_integrity_dict():
	integrity_dict = {'build' : {'36': 0, '37': 0, '38': 0, 'None': 0},
					  'sizes': [],
					  'columns': []
					  }

	return integrity_dict

def check_build(header:list):
	build = None
	for line in header:
		for item in line:
			if 'build' in item:
				if '36' in item:
					build = 36
				elif '37' in item:
					build = 37
				elif '38' in item:
					build = 38
				else:
					break
	return build

def get_file_size(filepath):
	size = os.path.getsize(filepath)

	return size


def main():

	cg_dir = '/Users/jerenolsen/Desktop/genome_downloads/CG_final_dir_oldnames'

	files = get_filepaths(cg_dir)

	integrity_dict = init_integrity_dict()


	for file in files:
		header, columns = read_header(file)

		### Check genome build ###
		build = check_build(header)
		integrity_dict['build'][str(build)] +=1

		### Check if column set is unique ###
		if columns not in integrity_dict['columns']:
			integrity_dict['columns'].append(columns)

		### Check if file size is acceptable ###
		fsize = get_file_size(file)
		integrity_dict['sizes'].append(round(fsize / (1024 * 1024 * 1024),3))


	print(f"Genome build counts: \n {integrity_dict['build']}\n")
	print(f"File size extremes: \n\t low end (GB): {sorted(integrity_dict['sizes'])[0:5]} \n\t high end (GB): {sorted(integrity_dict['sizes'])[-5:]} ")
	print(f"Unique column sets {len(integrity_dict['columns'])}")
	for col_set in integrity_dict['columns']:
		print(col_set, '\n')

if __name__=='__main__':
	main()

