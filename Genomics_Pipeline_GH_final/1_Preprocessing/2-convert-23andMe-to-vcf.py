# python3 2-convert-23andMe-to-vcf.py

import subprocess
import pandas as pd
import os
from google.cloud import storage
import numpy as np


def make_dir(path):
	if os.path.isdir(path):
		return 
	else:
		os.mkdir(path)

def get_storage_bucket(bucket_name, key_path):
	storage_client = storage.Client()
	bucket = storage_client.bucket(bucket_name)
	return bucket

def get_23andMe_files(input_metadata_path):
	files = []
	with open(input_metadata_path, 'r') as f:
		for line in f:
			line = line.strip('\n')
			files.append(line)
	files = [file for file in files if '23andMe' in file]
	return files

def get_build36_files(output_dir):
	files = os.listdir(output_dir)
	files = [file for file in files if 'b36' in file]
	return files

def run_bcftools(file, file_obj, temp_dir, build, resources_dir):
	fname = file.rsplit("/")[-1]
	fname = fname.rsplit(".")[0]+".vcf"

	sample_name = [item for item in fname.split('_') if 'hu' in item][0]

	ref_assembly = 'Homo_sapiens.GRCh37.dna.primary_assembly.fa'
	if build == 36:
		ref_assembly = 'Homo_sapiens.NCBI36.53.dna.toplevel.fa'


	genome_file = file_obj.download_as_text()
	genome_file_bytes = genome_file.encode("utf-8")

	bcftools_cmd = ["bcftools", "convert", "--tsv2vcf", "-", "-f", "{resources_dir}/reference_assemblies/{ref_assembly}".format(resources_dir=resources_dir,ref_assembly=ref_assembly), "-s", sample_name, "-Ov", "-o", "{output}/{vcf_name}".format(output=temp_dir, vcf_name=fname)]
	process = subprocess.run(bcftools_cmd, input=genome_file_bytes, capture_output=False)

	print("\n\t----\n")

def get_header_and_cols(path_23andMe):
	col_line = None
	header = []

	def iterate_lines(f, header, col_line):
		for line in f:
			if '#CHROM' in line or 'chromosome' in line:
				col_line = line
				break
			else:
				header.append(line) 
		return header, col_line

	if type(path_23andMe) == str:
		with open(path_23andMe, 'r') as f:
			header, col_line  = iterate_lines(f, header, col_line)
	else:
		with path_23andMe.open('r') as f:
			header, col_line  = iterate_lines(f, header, col_line)


	return header, col_line


def get_build(path_23andMe):
	build = None
	build_line = None

	header, __ = get_header_and_cols(path_23andMe)

	for line in header:
		if "are using" in line or 'reference used' in line or 'using human reference' in line:
			build_line = line

	if "36" in build_line:
		build = 36
	elif "37" in build_line:
		build = 37

	return build

def read_23andMe(path_23andMe):

	header, col_line = get_header_and_cols(path_23andMe)
                

	cols = col_line.strip('\n')
	cols = cols.strip('#').rsplit('\t')
	col_types = {col:str for col in cols}

	df = pd.read_csv(path_23andMe, sep='\t', names = cols, header = None, comment = '#', dtype=col_types)
	df = df.iloc[1:]
    
	return header, df

def filter_vcf_df(df, sample_name):
	#Remove non SNPs and mitochondrial genotypes
	#df = df[df['ALT'] != "."]

	df[sample_name] = np.where(df[sample_name] == ".", "./.", df[sample_name] )
	df = df[df['CHROM'] != 'MT']
	df = df[df['CHROM'] != 'X']
	df = df[df['CHROM'] != 'Y']

	return df

def write_vcf(file, header, build, df, output_dir):
	columns = df.columns.tolist()
	file_info = file.split('_')
	profile = [item for item in file_info if 'hu' in item][0]
	filename = '23andMe_{profile}_preprocessed_b{build}'.format(profile=profile, build=build)
	out_path = '{}/{}.vcf'.format(output_dir,filename)

	with open(out_path, 'w') as f:
		#Write Header
		for item in header: 
			f.write(item)

		#write columns
		f.write('#')
		for col in columns:
			if 'hu' not in col:
				f.write(col+'\t')
			else:
				f.write(col)
				f.write('\n')

	df.to_csv(r'{}'.format(out_path), header=False, index=None, sep='\t', mode='a')

def post_process_vcf(file, sample_name, temp_dir):
	fpath = os.path.join(temp_dir,file+".vcf")
	header, df = read_23andMe(fpath)
	df = filter_vcf_df(df, sample_name)

	return header, df

def convert_b36_files(output_dir, resources_dir):
	b36_files = get_build36_files(output_dir)
	for file in b36_files:
		filepath = os.path.join(output_dir,file)
		print(f"{file} is GRCh36, lifting over to GRCh37 ...")
		liftover(file, filepath, output_dir, resources_dir)

def liftover(filename, filepath, out_dir, resources_dir):
	filename = filename.split("_")[:-1]
	filename = "_".join(filename)+"_b37.vcf"

	liftover_script = "java -jar /home/jeren/pipeline-scripts/1_Preprocessing/liftover_tools/picard/build/libs/picard.jar LiftoverVcf  \
	I={filepath} \
	O={output}/{filename} \
	CHAIN={resources}/chain_files/NCBI36_to_GRCh37.chain \
	REJECT=/home/jeren/pipeline-scripts/1_Preprocessing/liftover_tools/rejected_variants.vcf \
	R={resources}/reference_assemblies/Homo_sapiens.GRCh37.dna.primary_assembly.fa \
	QUIET=true".format(filepath = filepath, output=out_dir, filename = filename, resources= resources_dir)

	liftover_script_command_list = liftover_script.split()
	print(f"liftover_script_command_list: {liftover_script_command_list}")
	process = subprocess.run(liftover_script_command_list, capture_output=False)

	os.remove(filepath)
	try:
		os.remove(out_dir+"/"+filename+".idx")
	except Exception as e:
		print("Exception in liftover(): {e}")

	print("\n\t----\n")

def get_sample_name(filename):
	file = filename.split('/')[-1]
	sample_name = [f for f in file.split('_') if 'hu' in f]

	return sample_name


def main():

	input_metadata_path = '/home/jeren/assigned_files.txt'
	temp_dir = '/home/jeren/pipeline-scripts/1_Preprocessing/temp_preprocessing'
	output_dir = '/home/jeren/pipeline-scripts/output_preprocessing'
	resources_dir = '/home/jeren/pipeline-scripts/resources' 

	make_dir(temp_dir)
	make_dir(output_dir)

	bucket_name = 'hvd-pgp-genomics-files'
	key_path = '/home/jeren/pipeline-scripts/keys/service-account-key.json'
	bucket = get_storage_bucket(bucket_name, key_path)

	files = get_23andMe_files(input_metadata_path)
	for file in files:
		file_obj = bucket.blob(file)
		build = get_build(file_obj)

		sample_name = get_sample_name(file)

		print(f"- Converting {file} to .vcf:")
		print(f"build: {build}")
		run_bcftools(file, file_obj, temp_dir, build, resources_dir)

		fname = file.rsplit('.')[0]
		fname = fname.split('/')[-1]
		print(f"- Postprocessing {fname} ...")
		header, df = post_process_vcf(fname, sample_name, temp_dir)

		print(f"- Writing {fname} ...")
		write_vcf(fname, header, build, df, output_dir)
		print("\t-Finished and written-\n")

	convert_b36_files(output_dir, resources_dir)

if __name__ =='__main__':
	main()