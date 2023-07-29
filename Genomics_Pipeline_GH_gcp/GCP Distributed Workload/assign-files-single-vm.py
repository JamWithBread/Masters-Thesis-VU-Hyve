from google.cloud import storage
import json
import yaml
from yaml.loader import SafeLoader


def list_bucket_files(key_path,bucket_name,files_dir):
	client = storage.Client.from_service_account_json(key_path)
	bucket = client.get_bucket(bucket_name)
	blobs = bucket.list_blobs(prefix=files_dir)

	file_list = [blob.name for blob in blobs if not blob.name.endswith('/')]

	return file_list

def extract_profile_names(string_list):
	#Assumes profiles in strings in string list are separated from other substrings by '/' and '_' characters only
	objects = []
	for item in string_list:
		items = item.split("/")
		for obj in items:
			objects.extend(obj.split('_'))

	profiles = [item for item in objects if 'hu' in item]
	return profiles


def get_assignable_files(key_path, bucket_name, all_files_dir, processed_files_dir): 

    all_files = list_bucket_files(key_path, bucket_name, all_files_dir)
    processed_files = list_bucket_files(key_path, bucket_name, processed_files_dir)

    all_profiles = extract_profile_names(all_files)
    processed_profiles = extract_profile_names(processed_files)

    assignable_profiles = [p for p in all_profiles if p not in processed_profiles]
    
    assignable_files = [file for file in all_files if any(profile in file for profile in assignable_profiles)]

    return assignable_files


def list_bucket_files_local(bucket_name):
	file_list = os.listdir(bucket_name)
	file_list = [file for file in file_list if ".DS_Store" not in file]

	return file_list

def read_instance_group_meta(yaml_path):
    with open(yaml_path) as f:
        group_info = list(yaml.safe_load_all(f))

    if type(group_info) == type(dict()):
    	group_info = [group_info]
    
    return group_info

def read_current_vm(txt_path):
    vm_id = None
    with open(txt_path, 'r') as f:
        for line in f:
            vm_id = line
            break
            
    return vm_id

def determine_vm_index(vm_id_path, group_info):
    vm_index = None
    vm_id = read_current_vm(vm_id_path)
    for i, item in enumerate(group_info):
        if item['id'] == vm_id:
            vm_index = i
    
    return vm_index

def assign_files(vm_index, group_info, all_files):
    # num_vms = len(group_info)
    # num_files = len(all_files)
    
    # remainder_files = False
    
    # files_per_vm = num_files/num_vms
    
    # if files_per_vm.is_integer() == False:
    #     files_per_vm = num_files//num_vms #round down
    #     remainder_start = files_per_vm*num_vms
    #     remainder_files = all_files[remainder_start:]
        
    # files_per_vm = int(files_per_vm)
        
    
    # start_index = vm_index * files_per_vm
    # stop_index = start_index + files_per_vm
    assigned_files = all_files
    
    # if remainder_files:
    #     if vm_index < len(remainder_files):
    #         assigned_files.append(remainder_files[vm_index])
            
    return assigned_files

def write_assigned_files(assigned_files:list, out_path):
	with open(out_path,'w') as f:
		for file in assigned_files:
			f.write(file+'\n')


	return True


def main():

	key_path = '/home/jeren/pipeline-scripts/keys/service-account-key.json'
	bucket_name = 'hvd-pgp-genomics-files'
	all_files_dir = 'input_genomes'
	processed_files_dir = 'final_output'

	assignable_files = get_assignable_files(key_path, bucket_name, all_files_dir, processed_files_dir)

	assigned_files_out_path = '/home/jeren/assigned_files.txt'


	instance_group_path = '/home/metadata/instance-group-info.yaml'
	vm_id_path = '/home/metadata/instance-id.txt'

	group_info = read_instance_group_meta(instance_group_path)
	vm_index = determine_vm_index(vm_id_path, group_info)

	assigned_files = assign_files(vm_index, group_info, assignable_files) 
	print(f"vm index: {vm_index}, assigned files: {len(assigned_files)} | {assigned_files}")

	write_assigned_files(assigned_files, assigned_files_out_path)

	print("- DONE -")
	


if __name__=='__main__':
	main()