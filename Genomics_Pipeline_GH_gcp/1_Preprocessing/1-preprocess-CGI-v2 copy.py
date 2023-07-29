# 1_preprocess_CGI_v2.py

import pandas as pd
import numpy as np
import os
import time
import itertools
import sys
from io import StringIO
from google.cloud import storage


def make_dir(path):
    if os.path.isdir(path):
        return 
    else:
        os.mkdir(path)

def get_storage_bucket(bucket_name, key_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return bucket

def get_cgi_files(input_metadata):
    files = None
    with open(input_metadata, 'r') as f:
        files = [line.strip('\n') for line in f]
    files = [file for file in files if 'CG' in file]
    return files

def filter_chunks(chunks, all_cols, remove_cols):
    filtered_chunks = []
    cols_keep = [col for col in all_cols if col not in remove_cols]
    i=0
    for chunk in chunks:
        i+=1
        filtered_chunks.append(chunk[cols_keep])
        if i %100 == 0:
            print(f"processed {i} chunks")
    return filtered_chunks

def read_CGI(file_obj, file_gs_url):

    tr0 = time.time()
    print(f"\tReading file...")

    col_line = None
    with file_obj.open('r') as f:
        for line in f:
            if '>' in line or 'locus' in line:
                col_line = col_line
                break

    cols = line.strip('\n').rsplit('\t')
    col_types = {col:str for col in cols}

    chunks = pd.read_csv(file_gs_url, sep='\t', names = cols, header = None, comment = '#', dtype=col_types, chunksize=100000)
    remove_cols = ['hapLink','alleleFreq','alternativeCalls'] #these columns are never used, remove early to save memory

    filtered_chunks = filter_chunks(chunks, cols, remove_cols)


    df = pd.concat(filtered_chunks)
    df = df.iloc[1:]

    tr1 = round(time.time()-tr0,2)
    print(f"\tFinished reading, took {tr1}s")

    return df

def get_header_build(file_obj):

    header_info = []

    def iterate_lines(f):
        header_info = []
        while True:
            line = f.readline()
            if line.startswith("#"):
                line = line.replace('\t', " ")
                line = line.replace('\n', "")
                header_info.append(line[1:])
            else:
                break

        return header_info

    print(f"file_obj: {file_obj}, type: {type(file_obj)}")

    if type(file_obj) == str:
        with open(file_obj, 'r') as f:
            headder_info = iterate_lines(f)

    else: #process as gcp file object
        with file_obj.open('r') as f:
            header_info = iterate_lines(f)
  
                
    build = None
            
    for x in header_info:
        if 'GENOME_REFERENCE' in x:
            if '36' in x:
                print('Build 36')
                build = 36
            elif '37' in x:
                print('Build 37')
                build = 37
            elif '38' in x:
                print("Build 38")
                build = 38
            else:
                build = 'Unknown Build'
                print("Unknown Build")

    return build 

def get_col_name(df, names):
    #returns the column string that has a matching search term in the names list

    cols = df.columns
    try:
        col_name = [col for col in cols if col in names][0]
    except Exception as e:
        return None
    return col_name

def filter_CGI(cgi_df):
    
    "Filter out low quality and unusable information from complete genomics file"
    
    df = cgi_df
    
    #Remove non-snps
    var_types = df['varType'].unique().tolist()
    var_types.remove('snp')
    var_types.remove('ref')
    remove_types = var_types

    df = df[df['reference'].str.contains('=') == False] #Genotype for stretch of positions is same as reference
    df = df[df['varType'].isin(remove_types) == False] #Exclude variant types that are not snp or reference equivalent
    df = df[df['reference'].str.len() == 1] #remove genotypes spanning multiple positions in a single line
    df = df[df['alleleSeq'].str.len() == 1] #remove genotypes spanning multiple positions in a single line
    
    #Remove low quality calls
    accepted_calls = ['PASS','VQHIGH']
    call_rating_col = get_col_name(df, ['varQuality','varFilter']) #call rating column has a different name in different CGI files
    if call_rating_col:
        df = df[df[call_rating_col].isin(accepted_calls) == True]
    
    #Remove Mitochondrial SNPs
    df = df[df['chromosome'].str.contains('M') == False]

    #Remove X and Y chromosomes
    df = df[df['chromosome'].str.contains('X') == False]
    df = df[df['chromosome'].str.contains('Y') == False]

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

def build_vcf_df2(cgi_df, filename):
    
    df = cgi_df
    
    #Rename columns
    df.rename(columns = {'xRef':'ID', 
                         'end':'POS', 
                         'reference':'REF', 
                         'alleleSeq':'ALT', 
                         'chromosome':'CHROM'}, inplace = True)
    
    sample_col_name = [item for item in filename.split("_") if 'hu' in item][0]
    
    #Create QUAL, FILTER, INFO, FORMAT cols
    df['QUAL'] = '.'
    df['FILTER'] = '.'
    df['INFO'] = '.'
    df['FORMAT'] = 'GT'
    
    #Remove loci with single occurence (all loci should have a partner, those with single occurence had partner removed in one of the previous steps)

    are_duplicates = df.duplicated('>locus', keep=False)
    df = df[are_duplicates]

    #Populate sample column with heterozygous/homozygous for alt allele 
    df['orig_ref_alt'] = df['REF'] + df['ALT'] #Temp column to store call type
  
    df_alt_count = df.groupby('>locus')['orig_ref_alt'].agg(list).reset_index() #Temp representation of main df where each row is a unique locus and contains call information of both variants

    df_alt_count['ref'] = df_alt_count['orig_ref_alt'].str.get(0)
    df_alt_count['alt'] = df_alt_count['orig_ref_alt'].str.get(1)
    df_alt_count['alt1'] = df_alt_count['alt'].str[1]
    df_alt_count['alt2'] = df_alt_count['ref'].str[1]
    df_alt_count['ref'] = df_alt_count['ref'].str[0]

    #Assign heterozygous or homozygous for alt
    df_alt_count[sample_col_name] = np.where(df_alt_count['alt1'] != df_alt_count['alt2'], '0/1', '1/1')
    #Assign homozygous for reference else keep previous assignment
    df_alt_count[sample_col_name] = np.where((df_alt_count['alt1'] == df_alt_count['alt2']) & (df_alt_count['alt1'] == df_alt_count['ref']), '0/0', df_alt_count[sample_col_name])

    #Add compressed representation of call back to original df
    df_alt_count = df_alt_count[['>locus', sample_col_name]]
    df = df.merge(df_alt_count, on='>locus', how='left')

    df['2nd_sort_col'] = np.where(df['REF'] != df['ALT'], 2, 1) #create sorting column, only want to keep copy of locus containing alternate allele calls
    df = df.sort_values(by=['>locus', '2nd_sort_col'], ascending = False) #Very important for duplicates filter in following step
    
    
    #Replace alt alleles that are actually reference with "."
    df['ALT'] = np.where(df['REF'] == '=', ".", df['ALT'])

    #Remove now redundant extra locus 
    are_duplicates = df.duplicated('>locus', keep='first')
    inverted = ~are_duplicates
    df = df[inverted]


    # Remove identical ref/alt pairs; these are a consequence of unique alternative calls for the same locus  (which one to keep is ambiguous)
    # Removing them earlier on would have been computationally expensive, and leaving them in until this point
    # Does not impact the other steps
    
    df = df[df['REF'] != df['ALT']]
    
    #Remove non-relevant columns
    df = df[['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT', sample_col_name]]
    df['CHROM'] = df['CHROM'].str[3:] #Remove 'chr' prefix
    
    df['POS'] = df['POS'].astype(int)
    df['CHROM'] = df['CHROM'].astype(int)
    df = df.sort_values(by=['CHROM','POS'])

    df.reset_index(drop=True, inplace=True)

    del df_alt_count
    
    return df


    
def save_file(df, input_file_path, output_path, build):
    #Save processed file as a 23andMe-like .txt file

    print(f"Saving File")

    new_header = ['fileformat=VCFv4.2',
                  'source=custom vcf transform script',
                  'reference={}'.format(build), 
                  'FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype>"']
                                  
    columns = df.columns.tolist()


    file_info = input_file_path.rsplit('/')[-1]
    file_info = file_info.rsplit('_')
    profile = [item for item in file_info if 'hu' in item][0]


    new_filename = 'CG_{profile}_preprocessed_b{build}'.format(profile=profile, build=build)
    out_path = '{}/{}.vcf'.format(output_path,new_filename)

    with open(out_path, 'w') as f:
        #Write Header
        for item in new_header: 
            f.write('##' + item +'\n')

        #Write Columns
        f.write('#')
        for col in columns:
            if 'hu' not in col:
                f.write(col+'\t')
            else:
                f.write(col)
                f.write('\n')

    df.to_csv(r'{}'.format(out_path), header=False, index=None, sep='\t', mode='a')

def main():

    input_metadata_path = '/home/jeren/assigned_files.txt'
    output_dir = '/home/jeren/pipeline-scripts/output_preprocessing' 

    bucket_name = 'hvd-pgp-genomics-files'
    key_path = '/home/jeren/pipeline-scripts/keys/service-account-key.json'
    bucket = get_storage_bucket(bucket_name, key_path)
    files = get_cgi_files(input_metadata_path)

    make_dir(output_dir)

    for filename in files:

        file_obj = bucket.blob(filename)
        file_gs_url = 'gs://{}/{}'.format(bucket_name, filename)
        print(f"file_gs_url: {file_gs_url}")

        t0 = time.time()
        print(f"Processing {filename}")
        df = read_CGI(file_obj, file_gs_url)
        df_filtered = filter_CGI(df)
        print(f"- Filtering Done")
        df_vcf = build_vcf_df2(df_filtered, filename)
        print(f"- Building Done")
        build = get_header_build(file_obj)
        save_file(df_vcf, file_gs_url, output_dir, build)
        t1 = round(time.time() - t0,2)
        print(f"- Done, took {t1} seconds\n")

        del df
        del df_filtered
        del df_vcf


if __name__ == '__main__':
    main()

