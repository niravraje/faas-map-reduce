from google.cloud import storage
from collections import defaultdict
import functions_framework
import json
import os

CS_SERVICE_ACCOUNT_KEY_PATH = f"secrets/nirav-raje-fall2022-cloud-storage-84ed9d82b477.json"
REDUCER_COUNT = 2

def invertedindex_reduce_init(reducer_input, reducer_id):
    print(f"[REDUCER - {reducer_id}] Inverted index reducing started...")
    reducer_output = {}
    for item in reducer_input:
        token, doc_name = item
        if token not in reducer_output:
            reducer_output[token] = []
        reducer_output[token].append(doc_name)
    # for key, val in reducer_output.items():
    #     reducer_output[key] = list(val)
    return reducer_output

def group_by_alphabet(reducer_count):
    letters = "abcdefghijklmnopqrstuvwxyz"
    groups = [""] * reducer_count
    i = 0
    for c in letters:
        groups[i % reducer_count] += c
        i += 1
    return groups

def get_reducer_input_from_cloud_storage(reducer_id, group):
    print(f"[REDUCER - {reducer_id}] Initializing Cloud Storage for {reducer_id}")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-mapper-output"
    bucket = storage.Bucket(storage_client, bucket_name)

    reducer_input_list = []

    blobs_list = storage_client.list_blobs(bucket_name)
    for blob in blobs_list:
        blob_content_bytes = blob.download_as_string()
        raw_reducer_input = json.loads(blob_content_bytes.decode("utf8"))
        token_to_doc_pairs = raw_reducer_input["default_mapper_key"]
        for item in token_to_doc_pairs:
            token, doc_name = item
            if token[0].lower() in group:
                reducer_input_list.append(item)

    return reducer_input_list

def upload_reducer_output_to_cloud_storage(reducer_id, reducer_output):
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-reducer-output"
    bucket = storage.Bucket(storage_client, bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
    filename = str(reducer_id) + ".json"
    blob = bucket.blob(filename)
    blob.upload_from_string(data=json.dumps(reducer_output), content_type="application/json")

@functions_framework.http
def reducer_init(request):
    reducer_id = request.args.get("reducer_id")
    print(f"{reducer_id} has started!")

    # groupby alphabet and reducer count
    groups = group_by_alphabet(REDUCER_COUNT)
    reducer_num = int(reducer_id[7:])
    curr_reducer_group = groups[reducer_num-1]
    print(f"[REDUCER - {reducer_id}] group string: {curr_reducer_group}")

    reducer_input_list = get_reducer_input_from_cloud_storage(reducer_id, curr_reducer_group)

    reducer_output = invertedindex_reduce_init(reducer_input_list, reducer_id)

    upload_reducer_output_to_cloud_storage(reducer_id, reducer_output)

    return {"status": "complete", "reducer_id": reducer_id}

    
