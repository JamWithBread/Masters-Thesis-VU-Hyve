# python3 download-PGP-genomes.py

import requests
from bs4 import BeautifulSoup
import multiprocessing
import time
import re
import wget
import os 
import shutil
import zipfile
import signal
import subprocess
import multiprocessing
from functools import partial

def read_txt(txt_path):
	contents = []
	with open(txt_path, 'r') as file:
		contents = [line.strip() for line in file]
	contents = [p.strip("\"") for p in contents]
	return contents

def get_profile_ids(provider):
	profiels = []
	profile_path = 'metadata/{}_profiles.txt'.format(provider)
	profiles = read_txt(txt_path=profile_path)

	return profiles

def get_base_url(provider):
	base_url = None
	if provider == 'CG': #Complete Genomics
		base_url = 'https://my.pgp-hms.org/public_genetic_data?utf8=✓&data_type=Complete+Genomics&commit=Search'
	elif provider == '23andMe':
		base_url = 'https://my.pgp-hms.org/public_genetic_data?utf8=✓&data_type=23andMe&commit=Search'

	return base_url

def get_dl_link_prefix(provider):
	dl_link_prefix = None
	if provider == '23andMe':
		dl_link_prefix = "https://my.pgp-hms.org"
	if provider == 'CG':
		dl_link_prefix = ""

	return dl_link_prefix

def get_download_target_string(provider):
	text = ''
	if provider == 'CG':
		text = 'http://evidence.pgp-hms.org/genome_download.php'
	elif provider == '23andMe':
		text = '/user_file/download'
	else:
		pass
	return text

def create_output_dir(out_dir):
	if not os.path.exists(out_dir):
		os.makedirs(out_dir)

def search_downloads_page(base_url, dl_target_string, dl_link_prefix, profile_ids):
	#Searches sequence provider page acquries genome download link for each valid profile_id
	page = requests.get(base_url)
	soup = BeautifulSoup(page.content, "html.parser")

	result = [a for a in soup.find_all('a', href=True)]

	profile_link_pairs = []
	profile = False
	dl_link = False
	for item in result:
		item = str(item)
		if 'profile' in item:
			p = item[18:26]
			if p not in profile_ids:
				continue
			else:
				profile = p

		if dl_target_string in item and profile != False:
			dl_link = item.rsplit("\"")[1]
			dl_link = dl_link_prefix + dl_link
			profile_link_pairs.append([profile, dl_link])
			profile = False
			dl_link = False


	return profile_link_pairs

def return_uploaded_data_hrefs(soup_object):
	soup = soup_object
	div = soup.div
	h3_section = soup.find('h3', text = 'Uploaded data ')
	p_div = h3_section.find_next('div')
	hrefs = p_div.find_all('a', href=True)

	return hrefs

def try_force_download_CG(i,link, out_dir, profile_id):

	#this string blocks access for some reason
	if 'amp;' in link:
		link = link.replace('amp;', "")

	print(f"Attempting force download CG: link: {link}")
	provider = 'CG'
	filename = "{i}_{provider}_{profile_id}_genome.tsv.bz2".format(i=i,provider = provider, profile_id = profile_id)
	filepath = os.path.join(out_dir, filename)
	try:
		wget.download(link, filepath)
		return True
	except Exception as e:
		print(f"Failed to force download cg link")
		return False


def download_genomes(provider, out_dir, i, profile_link_pairs):

	profile_id, link = profile_link_pairs[0], profile_link_pairs[1]

	response = requests.head(link)  # Send a HEAD request to get the response headers
	content_type = response.headers.get('content-type')

	print(f"\ni: {i} | Profile: {profile_id} | Content Type: {content_type} | Link: {link}")

	invalid_types = ['x-gzip', 'pdf', 'openxml', 'octet']
	if content_type == None or [x for x in invalid_types if x in content_type]:
		return

	ext = ""
	if 'html' in content_type:

		status = False
		if provider == 'CG':
			status = try_force_download_CG(i, link, out_dir, profile_id)
		if status == True:
			return None

		link = handle_html_content(link,provider, v=True)
		if link == None:
			return None
		ext = '.txt'

	if 'zip' in content_type or 'compress' in content_type:
		ext = '.zip'

	if 'text/plain' in content_type or 'None' in content_type: #'application/download', 'application/force-download'
		ext = '.txt'

	filename = "{i}_{provider}_{profile_id}_genome{ext}".format(i=i,provider = provider, profile_id = profile_id, ext = ext)
	filepath = os.path.join(out_dir, filename)

	print(f"\tDownloading {link}...")
	try:
		wget.download(link, filepath)
		print("\n\tDone!\n")
	except Exception as e:
		print(f"Error in wget: {e}")
		return None

def handle_arvados(soup):
	#Return download link from Arvados page
	body = soup.body
	base_link = None
	for item in body:
		item = str(item)
		if '--mirror' in item:
			item = item.split(" ")
			item = [i for i in item if 'https' in i][0]
			item = item.split('/_/')[0]
			item = item + '/_/'
			base_link = item

	return base_link

def handle_html_content(link, provider, v):

	def timeout_handler(signum, frame):
		raise TimeoutError("Function call timed out")

	def print_v(text,v):
		if v:
			print(text)

	timeout_seconds = 6
	if provider == 'CG':
		timeout_seconds = 60

	signal.signal(signal.SIGALRM, timeout_handler)
	signal.alarm(timeout_seconds)

	print_v("requesting html link",v)
	try:
		response = requests.get(link)
		signal.alarm(0)

	except (TimeoutError, requests.exceptions.RequestException) as e:
		print_v(f"\nAcquiring response from link [{link}] took too long or errored out",v)
		return None

	enc = response.encoding
	if enc != 'utf-8':
		print_v("Encoding not valid",v)
		return None

	line_1 = response.text.split("\n")[0]
	if line_1 != '<!DOCTYPE HTML>':
		print_v("response.text not valid",v)
		return None

	soup = BeautifulSoup(response.text, 'html.parser')

	dl_base_link = None
	dl_full_link = None

	if 'Arvados' in str(soup):
		print_v(f"This is an Arvados page, determined from str(soup)",v)
		dl_base_link = handle_arvados(soup)

	for anchor_tag in soup.find_all('a'):
		link = anchor_tag.get('href')

	if dl_base_link:
		dl_full_link = dl_base_link + link
		print_v(f"Full link: {dl_full_link}",v)

	return dl_full_link

def move_and_rename(source_dir, target_dir, new_name):
	files = os.listdir(source_dir)
	file = files[0]
	file = os.path.join(source_dir,file)
	destination_file = "{target_dir}/{new_name}".format(target_dir = target_dir, new_name=new_name)
	print(f"dest_file: {destination_file}")
	shutil.move(file, destination_file)

def run_function_parallel(function, iterable, *args):

	pool = multiprocessing.Pool(processes = 12) 
	partial_func = partial(function, *args)
	results = pool.starmap(partial_func, iterable)

	return results


def main():
	#### String Args ####
	provider = 'CG'
	base_url = get_base_url(provider)
	profile_ids = get_profile_ids(provider)
	dl_target_string = get_download_target_string(provider)
	dl_link_prefix = get_dl_link_prefix(provider)

	#### Scrape ####
	profile_link_pairs = search_downloads_page(base_url, dl_target_string, dl_link_prefix, profile_ids)
	profile_link_pairs = sorted(profile_link_pairs, key=lambda x:x[0])
	print(len(profile_link_pairs))

	#### Download ####
	current_dir = os.getcwd()
	out_dir = "{current_dir}/{provider}_genome_downloads".format(current_dir = current_dir, provider = provider)
	create_output_dir(out_dir)
	run_function_parallel(download_genomes, enumerate(profile_link_pairs), provider, out_dir)

	#### Run bash script to force unzip all files and move to final dir ####
	unzip_script = current_dir+'/unzip_{provider}.sh'.format(provider=provider)
	subprocess.call([unzip_script])


if __name__=='__main__':
	main()

