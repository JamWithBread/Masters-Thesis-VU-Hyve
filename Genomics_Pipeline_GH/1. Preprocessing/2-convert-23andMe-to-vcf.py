# python3 2-convert-23andMe-to-vcf.py

import subprocess
import pandas as pd
import os


def run_bcftools(input_path, temp_dir):
	fname = input_path.rsplit("/")[-1]
	fname = fname.rsplit(".")[0]+".vcf"
	bcftools_cmd = "bcftools convert --tsv2vcf {input_23andMe_txt} -f reference_assembly/Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz -s NA00001 -Ov -o {output}/{vcf_name}".format(input_23andMe_txt=input_path, output=temp_dir, vcf_name=fname)
	with open('bcftools-to-vcf.sh', 'w') as f:
		f.write(bcftools_cmd)
	subprocess.call(['sh', './bcftools-to-vcf.sh'])
	print("\n\t----\n")

def read_23andMe(path_23andMe):

	header = []

	col_line = None
	with open(path_23andMe, 'r') as f:
		for line in f:
			if '#CHROM' in line:
				col_line = col_line
				break
			else:
				header.append(line)
                

	cols = line.strip('\n')
	cols = cols.strip('#').rsplit('\t')
	col_types = {col:str for col in cols}

	df = pd.read_csv(path_23andMe, sep='\t', names = cols, header = None, comment = '#', dtype=col_types)
	df = df.iloc[1:]
    
	return header, df

def filter_vcf_df(df):
	#Remove non SNPs and mitochondrial genotypes
	df = df[df['ALT'] != "."]
	df = df[df['CHROM'] != 'MT']

	return df

def write_vcf(file, header, df, output_dir):
	columns = df.columns.tolist()
	filename = file.rsplit('_')[0:-1]
	filename = '_'.join(filename)+'_processed'
	out_path = '{}/{}.vcf'.format(output_dir,filename)

	with open(out_path, 'w') as f:
		#Write Header
		for item in header: 
			f.write(item)

		#write columns
		f.write('#')
		for col in columns:
			if col!= 'NA00001':
				f.write(col+'\t')
			else:
				f.write(col)
				f.write('\n')

	df.to_csv(r'{}'.format(out_path), header=False, index=None, sep='\t', mode='a')


def post_process_vcf(file, temp_dir, output_dir):
	fpath = os.path.join(temp_dir,file+".vcf")

	header, df = read_23andMe(fpath)
	df = filter_vcf_df(df)
	write_vcf(file, header, df, output_dir)


def main():
	input_path = '../input_genomes'
	temp_dir = 'temp'
	output_dir = 'output'
	files = os.listdir(input_path)
	for file in files:
		if "23andMe" in file:
			print(f"- Converting {file} to .vcf:")
			run_bcftools(os.path.join(input_path,file), temp_dir)

			fname = file.rsplit('.')[0]
			print(f"- Postprocessing {fname} ...")
			post_process_vcf(fname, temp_dir, output_dir)
			print("\t-Finished and written-\n")

if __name__ =='__main__':
	main()
