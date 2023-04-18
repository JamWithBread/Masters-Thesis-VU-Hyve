# python3 hvd-pgp-23andMe-retrieve-ehrs-surveys.py

"""
	using an input .txt file containing all participants on the Harvard PGP page under a given sequence provider (CGI or 23andME), 
	scrapes each participant's profile and accesses their EHR and survey data if present. Converts data to pandas df,
	and later writes dataframes to a directory for each participant in .tsv format

	Define input .txt in main()

"""

import csv
import requests
from bs4 import BeautifulSoup
from tabulate import tabulate
import pandas as pd
import multiprocessing
import time
import os


def load_profiles(text_file):
	"Expects text file delimited with '\t' as input"
	profiles = []
	with open(text_file, 'r') as f:
	 	for row in f:
	 		profile = row.split('\t')
	 		profile.pop()
	 		profiles.append(profile)
	return profiles

def get_ehrs_and_surveys(i, profile_entry:list):
	"""
	returns dataframes of EHR and Survey entries on profile's page
	input example : ['hu7EB316','EHR','Survey']
	"""

	print(f"{i}, processing profile: {profile_entry[0]}")
	profile_id = profile_entry[0]
	profile_base_url = 'https://my.pgp-hms.org/profile/{}'
	profile_url = profile_base_url.format(profile_id)
	page = requests.get(profile_url)
	soup = BeautifulSoup(page.content, "html.parser")

	ehrs = get_ehrs(profile_id, soup) if profile_entry[1] == 'EHR' else None
	surveys = get_surveys(profile_id, soup) if profile_entry[2] == 'Survey' else None

	return [profile_id, ehrs, surveys]

def get_ehrs(profile_id:str, soup):
	"Returns dataframes of all EHRs present for profile_id"

	h3 = soup.find_all('h3')
	h3 = [item for item in h3 if "Personal Health Records" in str(item)][0]
	div = h3.find_next('div')
	ehrs = pd.read_html(str(div))

	return ehrs

def get_surveys(profile_id:str, soup):
	"Returns a list of dataframes of all surveys present for profile_id"

	h3 = soup.find_all('h3')

	h3 = [item for item in h3 if "Surveys" in str(item)][0]

	div = h3.find_next('div')
	surveys = pd.read_html(str(div))

	"This section could be written more elegantly"
	survey_dfs = []
	cur_dict = dict()
	for index, row in surveys[0].iterrows():
		entry = row.values.tolist()
		if "Responses submitted" in str(row[1]):
			df = pd.DataFrame.from_dict(cur_dict, orient='index')
			df.reset_index(drop=True, inplace=True)
			survey_dfs.append(df.copy())
			cur_dict = dict()
		cur_dict[index] = entry

	"Finish last survey "
	df = pd.DataFrame.from_dict(cur_dict, orient='index')
	df.reset_index(drop=True, inplace=True)
	survey_dfs.append(df.copy())

	return survey_dfs

def create_profile_dir(profile_id:str):
	"Creates a directory for profile_id and returns the path"
	cur_dir = os.path.abspath(os.path.dirname(__file__))
	profile_dir = "all-data-by-profile/"+profile_id
	path = os.path.join(cur_dir, profile_dir)
	if not os.path.exists(path):
		os.mkdir(path)
	return path

def write_ehr_tsvs(ehrs,base_path):
	path = os.path.join(base_path,'ehrs')
	if not os.path.exists(path):
		os.mkdir(path)
	ehr_table_names = ['Demographic Information', 'Conditions', 'Medications', 'Allergies', 'Procedures', 'Test Results', 'Immunizations']
	i=0
	for ehr in ehrs:
		ehr.to_csv(path+'/'+ehr_table_names[i]+'.tsv', sep = '\t')
		i+=1


def write_survey_tsvs(surveys, base_path):
	path = os.path.join(base_path,'surveys')
	if not os.path.exists(path):
		os.mkdir(path)
	for survey in surveys:
		row1 = survey.values[:1].tolist()
		if row1 == []:
			continue
		survey_name = row1[0][0]
		if "/" in survey_name:
			survey_name = survey_name.replace('/', '_')

		survey.to_csv(path+'/'+survey_name+'.tsv', sep = '\t')

def populate_profile_dir(profile_id, ehrs, surveys):
	base_path = create_profile_dir(profile_id)
	if type(ehrs) == list:
		write_ehr_tsvs(ehrs, base_path)
	if type(surveys) == list:
		write_survey_tsvs(surveys, base_path)

	if type(surveys) != list and type(ehrs) != list:
		print(f"No EHR or Survey for participant :{profile_id}")


if __name__ == '__main__':

	#input_txt = 'PGP_HVD_CG_complete_profiles.txt'
	input_txt = 'PGP_HVD_23andMe_complete_profiles.txt'

	"Collect all EHR and survey data as dataframes for profiles, write to profile_id directory"
	t0 = time.time()
	print(f"\n- Scraping EHRs and Surveys for each participant, converting to dataframes")

	profile_list = load_profiles(input_txt)
	pool = multiprocessing.Pool(processes = 12) 
	results = pool.starmap(get_ehrs_and_surveys, enumerate(profile_list))
	print(f"\n- Scraping done, took {round((time.time() - t0)/60,2)} minutes")

	print(f"Length of results: {len(results)} items")

	"Write EHR and survey dfs to respective profile_id directories in .tsv format"
	t0 = time.time()

	print("\n- Writing all content to .tsv for each profile")
	for item in results:
		populate_profile_dir(item[0],item[1],item[2])
	print(f"\n- .tsv writing done, took {round((time.time() - t0)/60,2)} minutes ")



