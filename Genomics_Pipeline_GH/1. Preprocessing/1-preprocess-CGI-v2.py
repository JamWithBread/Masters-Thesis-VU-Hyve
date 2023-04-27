# 1_preprocess_CGI_v2.py

# TODO:
#	1. More versions of CGI files may throw new errors
#	2. Implement multiprocessing (starmap)

import pandas as pd
import numpy as np
import os
import re
import gc
import time
import itertools
from tabulate import tabulate

#Paths defined in main()

def read_CGI(cgi_path):

    tr0 = time.time()
    print(f"\tReading file...")

    col_line = None
    with open(cgi_path, 'r') as f:
        for line in f:
            if '>' in line or 'locus' in line:
                col_line = col_line
                break

    cols = line.strip('\n').rsplit('\t')
    col_types = {col:str for col in cols}

    df = pd.read_csv(cgi_path, sep='\t', names = cols, header = None, comment = '#', dtype=col_types)
    df = df.iloc[1:]

    tr1 = round(time.time()-tr0,2)
    print(f"\tFinished reading, took {tr1}s")

    return df

def get_header_build(file_path):
    
    with open(file_path,"r") as f:
        header_info = []
        while True:
            line = f.readline()
            if line.startswith("#"):
                line = line.replace('\t', " ")
                line = line.replace('\n', "")
                header_info.append(line[1:])
            else:
                break
                
        build = None
                
        for x in header_info:
            if 'GENOME_REFERENCE' in x:
                build = x
                if '36' in x:
                    print('Build 36')
                elif '37' in x:
                    print('Build 37')
                elif '38' in x:
                    print("Build 38")
                else:
                    build = 'Unknown Build'
                    print("Unknown Build")

        return build

def get_col_name(df, names):
    #returns the column string that has a matching search term in the list names

    cols = df.columns
    col_name = [col for col in cols if col in names][0]
    return col_name

def filter_CGI(cgi_df):
    
    "Filter out low quality and unusable information from complete genomics file"
    
    df = cgi_df
    
    #Remove non-snps
    var_types = df['varType'].unique().tolist()
    var_types.remove('snp')
    var_types.remove('ref')
    remove_types = var_types
    df = df[df['reference'].str.contains('=') == False] #Genotype is same as reference
    df = df[df['varType'].isin(remove_types) == False] #Exclude variant types that are not snp or reference equivalent
    df = df[df['reference'].str.len() == 1] #remove genotypes spanning multiple positions in a single line
    df = df[df['alleleSeq'].str.len() == 1] #remove genotypes spanning multiple positions in a single line
    
    #Remove low quality calls
    accepted_calls = ['PASS','VQHIGH']
    call_rating_col = get_col_name(df, ['varQuality','varFilter']) #call rating column has a different name in different CGI files
    df = df[df[call_rating_col].isin(accepted_calls) == True]
    
    #Remove Mitochondrial SNPs
    df = df[df['chromosome'].str.contains('M') == False]
    
    #Remove loci with single occurence (all loci should have a partner, those with single occurence had partner removed in one of the previous steps)
    are_duplicates = df.duplicated('>locus', keep=False)
    df = df[are_duplicates]
    
    #If rsid missing from external reference OR multiple rsids are present, replace with "."
    df['xRef'] = df['xRef'].fillna(".")
    df.loc[df['xRef'].str.contains(';'), 'xRef'] = '.'
    
    #Keep only rsid
    df['xRef'] = df['xRef'].str.split(':').str[1]
    df['xRef'] = df['xRef'].fillna(".")
    
    return df

def build_vcf_df(cgi_df):
    
    df = cgi_df
    
    #Rename columns
    df.rename(columns = {'xRef':'ID', 
                         'end':'POS', 
                         'reference':'REF', 
                         'alleleSeq':'ALT', 
                         'chromosome':'CHROM'}, inplace = True)
    
    #Create QUAL, FILTER, INFO, FORMAT and NA00001 col 
    df['QUAL'] = '.'
    df['FILTER'] = '.'
    df['INFO'] = '.'
    df['FORMAT'] = 'GT'
    df['NA00001'] = None

    #Populate NA00001 with heterozygous/homozygous for alt allele 
    df['orig_ref_alt'] = df['REF'] + df['ALT']
    df['ref_alt_offset'] = df['orig_ref_alt'].shift(1)
    df['NA00001'] = np.where(df['orig_ref_alt'] == df['ref_alt_offset'], '1/1', '0/1')

    #keep only keep alleles that show alt copy, remove allele that matches reference (in cases of heterozygous for SNP)
    df = df[df['varType'].str.contains('snp')]
    are_duplicates = df.duplicated('>locus', keep='last')
    inverted = ~are_duplicates
    df = df[inverted]
    
    #Remove non-relevant columns
    df = df[['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT', 'NA00001']]
    df['CHROM'] = df['CHROM'].str[3:] #Remove 'chr' prefix
    
    df.reset_index(drop=True, inplace=True)
    
    return df

    
def save_file(df, input_file_path, output_path):
    #Save processed file as a .vcf file

    build = get_header_build(input_file_path)
    new_header = ['fileformat=VCFv4.2',
                  'source=custom vcf transform script',
                  'reference={}'.format(build), 
                  'FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype>"']
                                  
    columns = df.columns.tolist()
    filename = input_file_path.rsplit('/')[-1]
    new_filename = '_'.join(filename.rsplit("_")[0:2])+'_preprocessed'
    out_path = '{}/{}.vcf'.format(output_path,new_filename)

    with open(out_path, 'w') as f:
        #Write Header
        for item in new_header: 
            f.write('##' + item +'\n')

        #Write Columns
        f.write('#')
        for col in columns:
            if col!= 'NA00001':
                f.write(col+'\t')
            else:
                f.write(col)
                f.write('\n')

    df.to_csv(r'{}'.format(out_path), header=False, index=None, sep='\t', mode='a')


def main():

	input_dir = '/Users/jerenolsen/Desktop/Genomics Pipeline/input_genomes/'
	output_dir = '/Users/jerenolsen/Desktop/Genomics Pipeline/1. Preprocessing/output/'

	for filename in os.listdir(input_dir):
		if 'CGI' in filename:
			t0 = time.time()
			print(f"Processing {filename}")
			df = read_CGI(input_dir+filename)
			df_filtered = filter_CGI(df)
			df_vcf = build_vcf_df(df_filtered)
			save_file(df_vcf, input_dir+filename, output_dir)
			t1 = round(time.time() - t0,2)
			print(f"- Done, took {t1} seconds\n")

		if '23andMe' in filename:
			pass

		else:
			print(f"Invalid filename: '{filename}', must indicate sequence provider")

if __name__ == '__main__':
	main()

