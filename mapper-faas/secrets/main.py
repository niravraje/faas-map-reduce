from google.cloud import storage
from collections import defaultdict
import functions_framework
import json
import os


CS_SERVICE_ACCOUNT_KEY_PATH = f"secrets/nirav-raje-fall2022-cloud-storage-84ed9d82b477.json"

def invertedindex_map_init(dataset, mapper_id):
    print(f"[MAPPER - {mapper_id}] Inverted index mapping started...")
    output = defaultdict(set)
    for doc in dataset:
        for line in dataset[doc]:
            tokens = line.split()
            for token in tokens:
                output["default_mapper_key"].add((token, doc))
    for key, val in output.items():
        output[key] = list(val)
    return output

def get_dataset_from_cloud_storage(mapper_id):
    print(f"[MAPPER - {mapper_id}] Initializing Cloud Storage for {mapper_id}")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-input-docs"
    bucket = storage.Bucket(storage_client, bucket_name)
    mapper_input_filename = "input-" + str(mapper_id) + ".json"
    blob = bucket.blob(mapper_input_filename)
    blob_content_bytes = blob.download_as_string()
    mapper_input_dataset = json.loads(blob_content_bytes.decode("utf8"))
    return mapper_input_dataset

def upload_mapper_output_to_cloud_storage(mapper_id, mapper_output):
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-mapper-output"
    bucket = storage.Bucket(storage_client, bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
    filename = str(mapper_id) + ".json"
    blob = bucket.blob(filename)
    blob.upload_from_string(data=json.dumps(mapper_output), content_type="application/json")

@functions_framework.http
def mapper_init(request):
    mapper_id = request.args.get("mapper_id")
    print(f"{mapper_id} has started!")

    mapper_input_dataset = get_dataset_from_cloud_storage(mapper_id)

    mapper_output = invertedindex_map_init(mapper_input_dataset, mapper_id)

    upload_mapper_output_to_cloud_storage(mapper_id, mapper_output)

    return {"status": "complete"}

    
