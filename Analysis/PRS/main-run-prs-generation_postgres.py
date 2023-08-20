import os
import shutil
import subprocess
import pandas as pd
import psycopg2

def get_pg_password(txt_path):
    with open(txt_path,'r') as f:
        for line in f:
            password=line.strip('\n')
    return password

def load_cohort_csv(path, case_size, control_size):

	#TODO: Refactor this, the dictionary approach doesnt make much sense when it could just be a dataframe, also to simplify having fewer source data directories

	df = pd.read_csv(path, sep='\t')
	df = df.dropna()

	cases = df[df['affected_status'] =='case']
	case_size = min(case_size, len(cases))
	cases = cases[0:case_size]

	controls = df[df['affected_status'] =='control']
	control_size = min(control_size, len(controls))
	controls = controls[0:control_size]

	cohort_dict = {'condition':[],
				   'control':[],
				   'sex': {}
					}

	cohort_dict['condition'] = cases['person_source_value'].values.tolist()
	cohort_dict['control'] = controls['person_source_value'].values.tolist()

	df = pd.concat([cases, controls])

	cohort_dict['sex'] = dict(zip(df['person_source_value'], df['gender_source_value']))

	return cohort_dict

def load_cohort_postgres(db_params, cohort_id, size_limit):

	query = """
	SELECT * FROM results.cohort 
	JOIN cdm.person 
	ON person.person_id = cohort.subject_id
	WHERE cohort_definition_id = {cohort_id}
	LIMIT {size_limit}
	""".format(cohort_id=cohort_id, size_limit = size_limit)

	try:

		connection = psycopg2.connect(**db_params)

		# Create a cursor to execute the query
		cursor = connection.cursor()

		cursor.execute(query)
		data = cursor.fetchall()
		column_names = [desc[0] for desc in cursor.description]

		df = pd.DataFrame(data, columns=column_names)

		cursor.close()
		connection.close()

		return df

	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL or executing query:", error)
		return None

def create_cohort_dict(df_cases, df_controls):

	def filter_cohort_df(df, status):
		df = df[['person_source_value', 'gender_source_value']]
		df['affected_status'] = status
		df.dropna(inplace=True)
		return df

	cases = filter_cohort_df(df_cases, 'case')
	controls = filter_cohort_df(df_controls, 'control')
	combined = pd.concat([cases,controls])

	cohort_dict = {'condition':[],
					'control':[],
					'sex': {}
					}

	cohort_dict['condition'] = cases['person_source_value'].values.tolist()
	cohort_dict['control'] = controls['person_source_value'].values.tolist()
	cohort_dict['sex'] = dict(zip(combined['person_source_value'], combined['gender_source_value']))

	return cohort_dict


def create_directory(path):
	if not os.path.exists(path):
		os.mkdir(path)
		print(f"directory created: {path}")
		return True

	return False


def create_cohort_files_dir(cohort_dict, input_dir, cohort_source_data_dir):

	if os.path.exists(cohort_source_data_dir):
		shutil.rmtree(cohort_source_data_dir)
	create_directory(cohort_source_data_dir)

	conditions_dir = os.path.join(cohort_source_data_dir, 'condition_source_data')
	control_dir = os.path.join(cohort_source_data_dir, 'control_source_data')
	create_directory(conditions_dir)
	create_directory(control_dir)

	all_files = os.listdir(input_dir)


	for file in all_files:
		if any(profile in file for profile in cohort_dict['condition']):
			source_filepath = os.path.join(input_dir,file)
			destination_filepath = os.path.join(conditions_dir,file)
			shutil.copy(source_filepath, destination_filepath)

		elif any(profile in file for profile in cohort_dict['control']):
			source_filepath = os.path.join(input_dir,file)
			destination_filepath = os.path.join(control_dir,file)
			shutil.copy(source_filepath, destination_filepath)

	return True

def update_cohort_dict(cohort_dict, cohort_source_data_dir):

	# Non optimal code

	data_dirs = os.listdir(cohort_source_data_dir)
	all_files = []
	for folder in data_dirs:
		path=os.path.join( cohort_source_data_dir,folder)
		_ = [all_files.append(f) for f in os.listdir(path)]

	existing_profiles = [f.split('_')[1] for f in all_files]

	updated_cohort_dict = cohort_dict

	profiles_to_remove_control = []
	profiles_to_remove_condition = []
	profiles_to_remove_sex = []

	for profile in cohort_dict['control']:
	    if profile not in existing_profiles:
	        profiles_to_remove_control.append(profile)

	for profile in cohort_dict['condition']:
	    if profile not in existing_profiles:
	        profiles_to_remove_condition.append(profile)

	for profile in cohort_dict['sex'].keys():
	    if profile not in existing_profiles:
	        profiles_to_remove_sex.append(profile)

	for profile in profiles_to_remove_control:
	    updated_cohort_dict['control'].remove(profile)

	for profile in profiles_to_remove_condition:
	    updated_cohort_dict['condition'].remove(profile)

	for profile in profiles_to_remove_sex:
	    del updated_cohort_dict['sex'][profile]


	return updated_cohort_dict

def save_cohort_info(cohort_dict, cohort_csv_outpath):
	#Save dataframe in output directory for ad hoc reference into the cohort details

	cases = pd.DataFrame()
	cases['profile_id'] = cohort_dict['condition']
	cases['affected_status'] = 'case'

	controls = pd.DataFrame()
	controls['profile_id'] = cohort_dict['control']
	controls['affected_status'] = 'control'

	df = pd.concat([cases,controls])
	df["gender"] = df["profile_id"].map(cohort_dict['sex'])

	df.to_csv(cohort_csv_outpath, sep = '\t')

	return True


def merge_source_vcfs(cohort_source_data_dir, merge_dir, temp_dir, ref_assembly_path, merged_file_output_path):

	if os.path.exists(temp_dir):
		shutil.rmtree(temp_dir)
	create_directory(temp_dir)

	# copy all .vcf files into temp dir
	for folder in os.listdir(cohort_source_data_dir):
		if 'source_data' not in folder:
			continue
		source_dir = os.path.join(cohort_source_data_dir, folder)
		for file in os.listdir(source_dir):
			source_filepath = os.path.join(source_dir,file)
			destination_filepath = os.path.join(temp_dir,file)
			shutil.copy(source_filepath, destination_filepath)

	# Reheader files (contig information missing)
	reheader_cmd = ["sh", "{merge_dir}/reheader.sh".format(merge_dir=merge_dir), "{merge_temp}".format(merge_temp=temp_dir), "{ref_assembly}".format(ref_assembly=ref_assembly_path)]
	process = subprocess.run(reheader_cmd)


	# Merge reheadered files
	merge_cmd = ["sh", "{merge_dir}/merge-vcf.sh".format(merge_dir=merge_dir), "{temp_dir}/merge_files_reheader".format(temp_dir=temp_dir), merged_file_output_path]
	process = subprocess.run(merge_cmd)

	#Delete Temp Merge Dir
	shutil.rmtree(temp_dir)

	return True

def generate_phenotype_file(cohort_dict, pheno_outpath):
	cols = ['FID', 'IID', 'GERD']

	profiles = cohort_dict['condition'] + cohort_dict['control']
	affected_mapping = {**{profile: 0 for profile in cohort_dict['control']}, **{profile: 1 for profile in cohort_dict['condition']}}

	with open(pheno_outpath,'w') as f:
		f.write("\t".join(cols)+'\n')
		for profile in profiles:
			f.write(profile+'\t')
			f.write(profile+'\t')
			f.write(str(affected_mapping[profile])+'\n')
	f.close()

	return True

def generate_plink_files(merged_cohort_vcf_path, plink_out_path):
	plink_cmd = ["sh", "plink-make-bed.sh", merged_cohort_vcf_path, plink_out_path]
	process = subprocess.run(plink_cmd)
	return True

def create_covariates(cohort_dict, base_path, cov_outpath):
	### Create Coviariates (.cov) file for cohort

	sex_mapping = {"male": 1, "female": 2}
	columns = ['FID', 'IID', 'Sex']
	with open(cov_outpath, 'w') as f:
		f.write("\t".join(columns)+'\n')
		for profile in cohort_dict['sex'].keys():
			sex_val = str(sex_mapping[cohort_dict['sex'][profile].lower()])
			f.write(profile+'\t')
			f.write(profile+'\t')
			f.write(sex_val+'\n')
	f.close()

	print(f"Covariates file written to {cov_outpath}")
	return True


def create_eigenvec(cohort_bfile_path, pruning_outpath, eigenvec_outpath):
	eigenvec_cmd = ['sh', 'get-eigenvec.sh', cohort_bfile_path, pruning_outpath, eigenvec_outpath]
	process = subprocess.run(eigenvec_cmd)										 
	### Generate first 6 principal components and store in .eigenvec file for use as covariates in regression model to account for population stratification
	return True

def combine_covariates_eigenvec(cov_fpath, eigenvec_path, covariates_outpath):

	eigenvec_path = eigenvec_path+".eigenvec"

	r_script = f"""
			covariate <- read.table("{cov_fpath}", header=T)
			pcs <- read.table("{eigenvec_path}", header=F)
			colnames(pcs) <- c("FID","IID", paste0("PC",1:6))
			cov <- merge(covariate, pcs, by=c("FID", "IID"))
			write.table(cov,"{covariates_outpath}", quote=F, row.names=F)
			q()
			"""
	process = subprocess.run(['Rscript', '-'], input=r_script, text=True, capture_output=True)

	if process.returncode == 0:
		print("Combine .cov + .eigenvec R script executed successfully.")
	else:
		print("Error executing the R script:")
		print(process.stderr)

	return True

def generate_covariate_PCs(cohort_dict, base_path, covariates_fname, plinked_file_path):
	cov_outpath = f"{base_path}/cohort.cov"
	eigenvec_output = f"{base_path}/eigenvec_output/cohort_eigenvec"
	combined_outpath = f"{base_path}/{covariates_fname}"

	create_covariates(cohort_dict, base_path, cov_outpath) ### Create Coviariates (.cov) file for cohort
	create_eigenvec(cohort_bfile_path = plinked_file_path, pruning_outpath = plinked_file_path, eigenvec_outpath = eigenvec_output) ### Generate first 6 principal components and store in .eigenvec file for use as covariates in regression model to account for population stratification
	combine_covariates_eigenvec(cov_fpath = cov_outpath, eigenvec_path = eigenvec_output, covariates_outpath = combined_outpath) ### Combine covariates and PCs

	return True

def run_prs_prsice2(prsice2_R_script, prsice2_binary, GWAS_path, target_data, phenotype_file, covariate, prs_outpath):
	prsice_cmd = ["sh", "PRSice-script.sh", prsice2_R_script, prsice2_binary, GWAS_path, target_data, phenotype_file, covariate, prs_outpath]
	process=subprocess.run(prsice_cmd)

	return True


def main(base_path):

	password=get_pg_password('/Users/jerenolsen/Desktop/password.txt')
	          
	db_params = {
	"host": "localhost",
	"port": "5432",
	"database": "etl-testing",
	"user": "postgres",
	"password": "{password}".format(password=password)
	}

	### Declare cases and controls cohort IDs ###
	cases_cohort_id = 5
	control_cohort_id = 6

	### Declare case and control sizes ###
	n_cases = 1000 #No max
	n_control = 188 # 2x cases

	### Program Control ###
	load_new_cohort = False
	run_merge_files = False
	run_plink = False
	generate_pheno = False
	create_covariates = False
	run_prs = True

	# Run all quickly
	#load_new_cohort = run_merge_files = run_plink = generate_pheno = create_covariates = run_prs = True
  
	input_dir = '/Users/jerenolsen/Desktop/GCP_final_output/final_output'
	cohort_source_data_dir = f"{base_path}/source_data"
	prs_output_dir = f"{base_path}/PRS_output"
	cohort_csv_outpath = f"{prs_output_dir}/prs_cohort_info.csv"
	create_directory(prs_output_dir)

	### Load cohort information and create source directory with .vcf files tied to cohort profiles ###

	print("Loading Cohort Information")
	df_cases = load_cohort_postgres(db_params, cohort_id = cases_cohort_id, size_limit = n_cases)
	df_controls = load_cohort_postgres(db_params, cohort_id = control_cohort_id, size_limit = n_control)

	cohort_dict = create_cohort_dict(df_cases, df_controls)

	if load_new_cohort:
		print("- Copying cohort files- ")
		create_cohort_files_dir(cohort_dict, input_dir, cohort_source_data_dir)


	# Old: loading from csv
	# cohort_info_file = f'{base_path}/GERD_cohort_all_data.csv'
	# cohort_dict = load_cohort(path=cohort_info_file, case_size=case_size, control_size=control_size)
	# if load_new_cohort:
	# 	print("Loading Cohort Information")
	# 	print("Copying cohort files")
	# 	create_cohort_files_dir(cohort_dict, input_dir, cohort_source_data_dir)

	### Update cohort dict, remove profiles which failed to retrieve .vcf file ###

	cohort_dict = update_cohort_dict(cohort_dict, cohort_source_data_dir)
	save_cohort_info(cohort_dict, cohort_csv_outpath)
	print(f"Number of initial cases with VCF file: {len(cohort_dict['condition'])}")
	print(f"Number of initial controls with VCF file: {len(cohort_dict['control'])}")

	### Merge source .vcfs into single .vcf with sample columns ###

	merge_dir = f'{base_path}/merge'
	temp_dir = f'{base_path}/merge/merge_temp'
	ref_assembly_path = f'{base_path}/merge/ref_assembly/Homo_sapiens.GRCh37.dna.primary_assembly.fa.fai'
	merged_file_output_path = f'{base_path}/merge/merged_cohort_files.vcf.gz'

	if run_merge_files:
		print("Merging cohort source .vcfs ...")
		merge_source_vcfs(cohort_source_data_dir, merge_dir, temp_dir, ref_assembly_path, merged_file_output_path)


	### Make Plink Files (.bed, .bim, .fam.)

	merged_cohort_vcf_path = merged_file_output_path
	plink_out_path = f"{base_path}/plink_output/cohort_plinked"

	if run_plink:
		print(f" - Generating plink files for target data - ")
		generate_plink_files(merged_cohort_vcf_path, plink_out_path)

	### Generate Phenotype File ###

	pheno_outpath = f"{base_path}/cohort_phenotypes.pheno"
	if generate_pheno:
		print(f" - Creating phenotype file: {pheno_outpath}")
		generate_phenotype_file(cohort_dict, pheno_outpath)

	### Create combined covariates (sex) and eigenvec (first 6 PCs) file ###

	covariates_fname = "cohort_covariates.covariate"
	if create_covariates:
		print(f" - Creating combined covariates file: {covariates_fname}")
		generate_covariate_PCs(cohort_dict, base_path, covariates_fname, plink_out_path) #Some paths are defined in this fn

	### Generate the PRS Using PRSice2 ###

	if run_prs:
		prsice2_R_script = f"{base_path}/PRSice_mac/PRSice.R"
		prsice2_binary = f"{base_path}/PRSice_mac/PRSice_mac"
		GWAS_path = f"{base_path}/base_data/UKB_GWAS_SumStats_GERD_processed.txt"
		target_data = plink_out_path
		phenotype_file = pheno_outpath
		covariate = f"{base_path}/{covariates_fname}"
		prs_outpath = f"{prs_output_dir}/cohort_prs"
		run_prs_prsice2(prsice2_R_script, prsice2_binary, GWAS_path, target_data, phenotype_file, covariate, prs_outpath)


if __name__ == '__main__':
	base_path= '/Users/jerenolsen/Desktop/All_Tests/PRSice_Testing/Test_Run'
	subprocess.run(["find", base_path, '-type', 'f', '-name', '.DS_Store', '-delete'])
	main(base_path)

