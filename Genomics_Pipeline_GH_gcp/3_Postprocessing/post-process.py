import pandas as pd
import time
import os
import io
from google.cloud import storage
import gc

#TODO: Implement multiprocessing (starmap)

#Input and output paths defined in main()

def get_storage_blob(bucket_name, directory_path, filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(directory_path+filename)
    return blob

def get_file_paths(input_dir):
    files = os.listdir(input_dir)
    files = [file for file in files if 'hu' in file]
    paths = [os.path.join(input_dir,file) for file in files]
    return paths

def read_vcf(vcf_file):

    header = []
    col_line = None
    with open(vcf_file, 'r') as f:
        for line in f:
            if '#CHROM' in line:
                col_line = col_line
                break
            else:
                header.append(line)

    cols = line.strip('\n')
    cols = cols.strip('#').rsplit('\t')
    col_types = {col:str for col in cols}

    df = pd.read_csv(vcf_file, sep='\t', names = cols, header = None, comment = '#', dtype=col_types)
    # chunks = pd.read_csv(vcf_file, sep='\t', names = cols, header = None, comment = '#', dtype=col_types, chunksize=100000)
    # df = pd.concat(chunks)

    return header, df


def post_process(df):
    
    #Filter out Non-SNPs
    df = df[df['REF'].str.len() == 1]
    df = df[df['ALT'].str.len() == 1]
   
    #Filter out variants with MAF scores < 0.05
    af_col = df['INFO'].str[12:18]
    af_filter = af_col.astype(float) > 0.05
    df = df[af_filter]
    
    #Remove variants with missing or unknown external reference
    df = df[df['ID'] != "."]
 
    
    #Fix index
    df.reset_index(drop = True, inplace = True)

    #sort layered sort by chromosome and position
    df['POS'] = df['POS'].astype(int)
    df = df.sort_values(['CHROM', 'POS'])

    return df
    

def write_vcf_to_blob(file, header, df, bucket_name, directory_path):
    columns = df.columns.tolist()
    filename = file.rsplit('/')[-1]
    filename = filename.rsplit('_')[0:-1]
    filename = '_'.join(filename)+'_final.vcf'

    blob = get_storage_blob(bucket_name, directory_path, filename)

    csv_buffer = io.StringIO()

    # Write Header
    for item in header:
        csv_buffer.write(item)

    #write columns
    csv_buffer.write('#')
    for col in columns:
        if 'hu' not in col:
            csv_buffer.write(col+'\t')
        else:
            csv_buffer.write(col)
            csv_buffer.write('\n')

    df.to_csv(csv_buffer, header=False, index=None, sep='\t', mode='a')
    csv_data = csv_buffer.getvalue().encode('utf-8')  # Convert content to bytes
    blob.upload_from_string(data=csv_data, content_type='text/csv')


def main():
    input_dir = '/home/jeren/pipeline-scripts/output_imputation/'
    output_dir = 'final_output/'
    vcf_files = get_file_paths(input_dir)

    bucket_name = 'hvd-pgp-genomics-files'
   # output_blob = get_storage_blob(bucket_name = bucket_name , directory_path = output_dir)

    for file in vcf_files:
        try:
            t0=time.time()
            print(f"Processing {os.path.basename(file)}...")

            header, df_vcf = read_vcf(file)
            print(f"\t - Finished Reading ")

            df_vcf_p = post_process(df_vcf)
            print(f"\t - Finished Filtering ")

            write_vcf_to_blob(file, header, df_vcf_p, bucket_name, directory_path = output_dir)
            print(f"\t - File written to output in bucket {bucket_name}, location: {output_dir}\n\t- Time: {round(time.time()-t0,2)}s")

            del df_vcf
            del df_vcf_p
            gc.collect()

        except Exception as e:
            print(f"File {file} failed postprocess")
            gc.collect()
            continue

if __name__ == '__main__':
	main()
