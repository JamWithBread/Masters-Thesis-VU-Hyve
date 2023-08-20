from google.cloud import bigquery
import pandas as pd
from pandas.io.json import json_normalize


def read_variants_table(variants_path):
    sample = pd.read_csv(variants_path, header=0, nrows = 2, sep = '\t')
    cols = sample.columns
    dtypes = {col:str for col in cols}
    df = pd.read_csv(variants_path, names = cols, dtype = dtypes, sep = '\t', comment = '#')

    return df 

def filter_variants(df):
    # Filter variants table to return only medically significant variants (pathogenic and likely pathogenic)
    # Also to match the fitlers applied to sample data (autosomes only, SNPs only, etc)

    # SNPs only
    df = df[df['Type'] == 'single nucleotide variant']
    df = df[df['ReferenceAlleleVCF'].str.len() == 1]
    df = df[df['AlternateAlleleVCF'].str.len() == 1]

    # Filter for P/LP variants
    accepted_significance = ['Pathogenic','Likely pathogenic']
    df = df[df['ClinicalSignificance'].isin(accepted_significance)]
    
    # Only consider positions from GRCh37 Assembly
    df = df[df['Assembly'] == 'GRCh37']
    
    # Remove X,Y, MT chromosomes
    df = df[~df['Chromosome'].isin(['X','Y','MT'])]
    
    df.reset_index(drop=True, inplace=True)
    
    return df

def get_chr_rsids(variants_df, chr_num):
    df_chrx = variants_df[variants_df['Chromosome'] == str(chr_num)]
    rsids = df_chrx['RS# (dbSNP)'].unique().tolist()
    rsids = ['rs'+val for val in rsids]

    return rsids

def init_chr_dict():
    chr_list = [num for num in range(1,23)]
    chr_dict = {item:pd.DataFrame() for item in chr_list}
    return chr_dict


def create_GBQ_variants_query(project_id, dataset, table_name, chr_num, rsids_list):

    rsids_list =', '.join(["'{}'".format(rsid) for rsid in rsids_list]) #Removes square brackets from list

    query = """SELECT *
            FROM `{project_id}.{dataset}.{table_name}{chr_num}` as v, UNNEST(v.call) as c
            WHERE EXISTS (
                  SELECT *
                  FROM UNNEST(names) AS name
                  WHERE name IN ({rsids_list})
                  AND EXISTS (SELECT g FROM UNNEST(c.genotype) AS g WHERE g > 0))""".format(project_id=project_id, dataset=dataset, table_name=table_name, chr_num=chr_num, rsids_list=rsids_list)

    return query

def create_GBQ_connection():
    client = bigquery.Client()
    return client

def execute_GBQ_query(query, client):
    query_job = client.query(query)
    query_results = query_job.result()
    return query_results

def query_result_to_df(query_results):
    df = query_results.to_dataframe()

    # Normalize nested columns and create new columns
    df = df.explode('alternate_bases')
    df = pd.concat([df.drop('alternate_bases', axis=1),
                    pd.json_normalize(df['alternate_bases']).add_prefix('alternate_bases.')],
                   axis=1)

    df = df.explode('call')
    df = pd.concat([df.drop('call', axis=1),
                    pd.json_normalize(df['call']).add_prefix('call.')],
                   axis=1)

    # Unpack row values in columns containing lists
    df['names'] = df['names'].str.get(0)
    df['filter'] = df['filter'].str.get(0)
    df['genotype'] = df['genotype'].astype(str).str[1]+"/"+ df['genotype'].astype(str).str[3] 
    df['DS'] = df['DS'].str.get(0)

    return df

def run_GBQ_variants_query(client, project_id, bq_dataset_id, bq_tablename, chr_num, rsids_list):

    query = create_GBQ_variants_query(project_id, dataset=bq_dataset_id , table_name=bq_tablename, chr_num=chr_num, rsids_list=rsids_list)
    query_results = execute_GBQ_query(query, client)

    return query_results


def create_main_variants_df(chr_dict):
    main_df = pd.DataFrame()
    for key in chr_dict.keys():
        df = chr_dict[key]
        df['chromosome'] = key
        main_df = pd.concat([main_df, df])

    main_df.reset_index(drop=True, inplace=True)

    # Reorder columns and remove redundant: 'reference_name', 'call.sample_id', 'call.genotype', 'call.phaseset', 'call.DS'
    cols_reordered = ['chromosome', 'start_position', 'end_position', 'reference_bases', 'alternate_bases.alt', 'alternate_bases.AF',
       'alternate_bases.DR2',
       'names', 'quality', 'filter', 'IMP', 'sample_id', 'genotype',
       'phaseset', 'DS']

    main_df = main_df[cols_reordered]

    return main_df


def get_sample_id_mappings(project_id, bq_dataset_id, bq_tablename, client):

    query = """SELECT * FROM `{project_id}.{bq_dataset_id}.{bq_tablename}`""".format(project_id=project_id, bq_dataset_id=bq_dataset_id, bq_tablename=bq_tablename)
    query_results = execute_GBQ_query(query, client)
    df = query_results.to_dataframe()
    df['sample_name'] = df['sample_name'].str[-8:]

    return df

def add_profile_column(df_main, id_profiles_df ):
    df_main = df_main.merge(id_profiles_df, how = 'left', on='sample_id')
    keep_and_reorder = ['chromosome', 'start_position', 'end_position',
       'reference_bases', 'alternate_bases.alt', 'alternate_bases.AF',
       'alternate_bases.DR2', 'names', 'quality', 'filter', 'IMP',
       'genotype', 'phaseset', 'DS', 'sample_name','sample_id']
    df_main = df_main[keep_and_reorder]

    return df_main

def write_df_to_csv(df, outpath, delim):
    df.to_csv(outpath, sep=delim, index=False)
    print(f"Dataframe written to {outpath}")
    return True


def main():
    clinvar_variants_path = '/Users/jerenolsen/Desktop/All_Tests/HVD-PGP Population Analysis/Deep Phenotyping copy/ClinVar Variants/variant_summary.txt'

    project_id = 'hyve-ohdsi-genomics-masters'
    bq_dataset_id = 'hvd_pgp_participant_variants'
    bq_chr_tablename = 'hvd_pgp_variants__chr'
    bq_samples_tablename = 'hvd_pgp_variants__sample_info'

    outpath = '/Users/jerenolsen/Desktop/hvd-pgp-ms-population-variants.tsv'

    gbq_client = create_GBQ_connection()

    clinvar_variants = read_variants_table(clinvar_variants_path)
    cv_df = filter_variants(clinvar_variants)

    chr_dict = init_chr_dict()

    for chr_num in chr_dict.keys():
        rsids_list = get_chr_rsids(cv_df, chr_num)
        print(f"Running query for chromosome {chr_num}")
        query_results = run_GBQ_variants_query(gbq_client, project_id, bq_dataset_id, bq_chr_tablename, chr_num, rsids_list)
        chrx_df = query_result_to_df(query_results)
        chr_dict[chr_num] = chrx_df

    main_df = create_main_variants_df(chr_dict)
    id_profiles_df = get_sample_id_mappings(project_id, bq_dataset_id, bq_samples_tablename, gbq_client)
    main_df = add_profile_column(main_df, id_profiles_df)

    write_df_to_csv(main_df, outpath, delim = '\t')


if __name__ == '__main__':
    main()


