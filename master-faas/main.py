from google.cloud import storage
import os
import json
import string
from utils import *
import functions_framework
from threading import Thread
import requests

# Should replace with a config file later
CS_SERVICE_ACCOUNT_KEY_PATH = f"secrets/nirav-raje-fall2022-cloud-storage-84ed9d82b477.json"
RAW_INPUT_DATA_PATH = f"/tmp/rawdataset"
MAPPER_COUNT = 3
REDUCER_COUNT = 2
MAPPER_FAAS_URL = "https://mapper-cf-gf2co4zquq-uc.a.run.app"
REDUCER_FAAS_URL = "https://reducer-cf-gf2co4zquq-uc.a.run.app"

def mapper_trigger(mapper_id):
    response = requests.get(MAPPER_FAAS_URL, params={"mapper_id": str(mapper_id)})

def reducer_trigger(reducer_id):
    response = requests.get(REDUCER_FAAS_URL, params={"reducer_id": str(reducer_id)})

@functions_framework.http
def master_init(request):
    print("[#] Initializing Cloud Storage...")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-raw-dataset"
    bucket = storage.Bucket(storage_client, bucket_name)

    if not bucket.exists():
        return "Raw dataset bucket doesn't exist"

    blobs_list = storage_client.list_blobs(bucket_name)

    os.makedirs(RAW_INPUT_DATA_PATH, exist_ok=True)

    if os.path.exists(RAW_INPUT_DATA_PATH):
        print("Path exists!")

    for blob in blobs_list:
        blob_data = blob.download_as_string().decode("utf8")
        # print("blob data:", blob_data)
        # blob.download_to_filename(f"/tmp/raw-dataset/{blob.name}")
        filename = os.path.join(RAW_INPUT_DATA_PATH, str(blob.name))
        print(f"Writing {filename}")
        with open(filename, "w") as fp:
            fp.write(blob_data)

    dataset = generate_dataset(RAW_INPUT_DATA_PATH)

    upload_mapper_input_to_cloud_storage(dataset)

    mapper_threads = []
    for i in range(1, MAPPER_COUNT+1):
        mapper_id = f"mapper{i}"
        curr_thread = Thread(target=mapper_trigger, args=(mapper_id,))
        mapper_threads.append(curr_thread)
        curr_thread.start()
    
    # Barrier: wait for all mapper cloud functions to complete
    for curr_thread in mapper_threads:
        curr_thread.join()
    
    reducer_threads = []
    for i in range(1, REDUCER_COUNT+1):
        reducer_id = f"reducer{i}"
        curr_thread = Thread(target=reducer_trigger, args=(reducer_id,))
        reducer_threads.append(curr_thread)
        curr_thread.start()
    
    # Wait for all reducer cloud functions to complete
    for curr_thread in reducer_threads:
        curr_thread.join()

    # Combine all reducer output files into a single final output file
    final_output = combine_reducer_output_files()
    upload_final_output_to_cloud_storage(final_output)

    return {"master_status": "complete"}