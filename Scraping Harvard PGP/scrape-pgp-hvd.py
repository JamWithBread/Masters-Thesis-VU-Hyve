# python3 scrape-pgp-hvd-23andMe.py

import requests
from bs4 import BeautifulSoup
import multiprocessing
import time


"""
Scrapes the PGP Harvard site on the INSERT(sequence provider) genomics page
Main Steps:
1. Get all Profile (participant) Ids from base_url that have a downloadable genome file
2. Go to all profile pages and check if there is an EHR and/or a survey
3. Save identified participants to txt file along with whether or not EHR / survey data is present
"""

### Run for Complete Genomics ##
#provider = 'CG'
#base_url = 'https://my.pgp-hms.org/public_genetic_data?utf8=✓&data_type=Complete+Genomics&commit=Search' #Complete Genomics
#output_txt = 'PGP_HVD_CG_complete_profiles.txt'

### Run for 23andMe ##
provider = '23andMe'
base_url = 'https://my.pgp-hms.org/public_genetic_data?utf8=✓&data_type=23andMe&commit=Search'
output_txt = 'PGP_HVD_23andMe_complete_profiles.txt'

def get_download_link_string(provider):
	text = ''
	if provider == 'CG':
		text = 'http://evidence.pgp-hms.org/genome_download.php'
	elif provider == '23andMe':
		text = '/user_file/download'
	else:
		pass
	return text

def display(input):
	if type(input) == list:
		for item in input:
			print(item)
		print(f"list of length {len(input)}")

def check_download(href_profile):
	"For current href profile on page, check if the next href if a download link to the genome file"
	download_text = get_download_link_string(provider)
	for href in href_profile.find_all_next('a', href=True):
		if download_text in str(href):
			return True
		break
	return False

def find_all_profiles(base_url):
	"Returns a list of profile ids that have a genome download link"
	page = requests.get(base_url)
	soup = BeautifulSoup(page.content, "html.parser")

	result1 = [a for a in soup.find_all('a', href=True)]
	result2 = []
	for item in result1:
		if '/profile' not in str(item):
			continue
		if check_download(item):
			result2.append(item)
	profiles = [str(a)[18:26] for a in result2]

	return profiles


def check_for_ehr(profile_page_soup):

	last_marked = profile_page_soup.find('h3')
	for tag in last_marked.find_all_next("div"):
		try:
			check_phr = tag.div['class']
			if check_phr[0] == 'phr':
				return "EHR"
			break

		except Exception as e:
			#print(f"Exception in check_for_ehr: {e}")
			pass
	return 'None'


def check_for_survey(profile_page_soup):
	start = profile_page_soup.find('h3')
	for tag in start.find_all_next("h3"):
		try:
			if tag.text == "Surveys":
				for div in tag.find_all_next("div"):
						if div['class'][0] == "profile-data":
							return "Survey"

		except Exception as e:
			pass
			#print(f"Exception in check_for_survey(): {e}")

	return 'None'

def check_for_ehr_survey(i, profile_id):
	"Checks if there is a ehr div and survey div"
	print(profile_id, i, 'checking for ehr and survey')

	base_profile_url = 'https://my.pgp-hms.org/profile/{profile}'
	profile_url = base_profile_url.format(profile = profile_id)

	page = requests.get(profile_url)
	soup = BeautifulSoup(page.content, "html.parser")
	
	ehr = check_for_ehr(soup)
	survey = check_for_survey(soup)

	return [profile_id, ehr, survey]

def write_to_txt(id_list, filename):
	with open(filename, 'w') as f:
		for triple in id_list:
			for item in triple:
				if item!= str:
					item = str(item)
				f.write(item+'\t')
			f.write('\n')

if __name__ == '__main__':

	"get complete profiles"
	t0 = time.time()

	wgs_profiles = find_all_profiles(base_url)
	print(f"{len(wgs_profiles)} wgs profiles collected\n")

	pool = multiprocessing.Pool(processes = 16) 

	complete_profiles = pool.starmap(check_for_ehr_survey, enumerate(wgs_profiles))

	#complete_profiles = [id for id in complete_profiles if id != None]

	write_to_txt(complete_profiles, output_txt)

	print(f"Done, process took {round((time.time() - t0)/60,2)} minutes")

	display(complete_profiles)

	has_ehrs = [ehr for ehr in complete_profiles if ehr[1] != 'None']
	display(has_ehrs)
