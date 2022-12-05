import os
import json
import string
from google.cloud import storage

CS_SERVICE_ACCOUNT_KEY_PATH = f"secrets/nirav-raje-fall2022-cloud-storage-84ed9d82b477.json"
RAW_INPUT_DATA_PATH = f"/tmp/rawdataset"
MAPPER_COUNT = 3
REDUCER_COUNT = 2

def cleanup_lines_list(doc_lines_list):
    # remove punctuations 
    punctuation_set = set(string.punctuation)
    for i in range(len(doc_lines_list)):
        doc_lines_list[i] = doc_lines_list[i].translate(str.maketrans('', '', string.punctuation))
    
    # strip whitespaces and newline chars
    doc_lines_list = [doc_lines_list[i].strip() for i in range(len(doc_lines_list))]
    
    # remove blank lines
    doc_lines_list = [doc_lines_list[i] for i in range(len(doc_lines_list)) if doc_lines_list[i]]
    
    # convert all words to lower case
    for i in range(len(doc_lines_list)):
            doc_lines_list[i] = doc_lines_list[i].lower()
            line_encode = doc_lines_list[i].encode("ascii", "ignore")
            doc_lines_list[i] = line_encode.decode()
    return doc_lines_list

def generate_dataset(raw_input_data_path):
    dataset = {}
    for filename in os.listdir(raw_input_data_path):
        filepath = os.path.join(raw_input_data_path, filename)
        print(f"[#] Filepath: {filepath}")
        with open(filepath, "r") as fp:
            doc_lines_list = fp.readlines()
        doc_lines_list = cleanup_lines_list(doc_lines_list)
        dataset[filename] = doc_lines_list
    return dataset

def count_lines_in_dataset(dataset):
    count = 0
    for doc in dataset:
        count += len(dataset[doc])
    return count

def create_partitioned_dataset(dataset, mapper_count):
    result_dataset = {}
    for mapper_num in range(mapper_count):
        mapper_id = "mapper" + str(mapper_num+1)
        result_dataset[mapper_id] = {}

    total_lines = count_lines_in_dataset(dataset)
    mapper_chunk_size = total_lines // mapper_count

    mapper_num = 1
    mapper_id = "mapper" + str(mapper_num)
    curr_chunk_size = mapper_chunk_size
    start = 0
    mapper_lines_count = 0
    doc_names = list(dataset.keys())

    i = 0
    while i < len(doc_names):

        doc = doc_names[i]
        end = start + curr_chunk_size

        # if end is within doc size limits and this is not the last mapper
        if end < len(dataset[doc]) and mapper_num != mapper_count:
            result_dataset[mapper_id][doc] = dataset[doc][start:end]
            mapper_lines_count += len(dataset[doc][start:end])
            curr_chunk_size = mapper_chunk_size
            start = end
        else:
            result_dataset[mapper_id][doc] = dataset[doc][start:]
            curr_chunk_size = mapper_chunk_size - len(dataset[doc][start:])
            mapper_lines_count += len(dataset[doc][start:])
            start = 0
            i += 1

        if mapper_lines_count >= mapper_chunk_size:
            mapper_num += 1
            mapper_id = "mapper" + str(mapper_num)
            mapper_lines_count = 0
    
    print("\n\n\n-- [INFO] Partitioned Dataset Summary --")
    for mapper_id in result_dataset:
        print(f"\n[{mapper_id}]")
        print(f"Total lines in mapper: {count_lines_in_dataset(result_dataset[mapper_id])}")
        print(f"Docs in mapper: {result_dataset[mapper_id].keys()}")
    print("\n-- End of Partitioned Dataset Summary --\n\n\n")
        
    return result_dataset

def upload_mapper_input_to_cloud_storage(dataset):
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-input-docs"
    bucket = storage.Bucket(storage_client, bucket_name)

    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
    
    partitioned_dataset = create_partitioned_dataset(dataset, MAPPER_COUNT)

    for mapper_id in partitioned_dataset:
        mapper_json_data = partitioned_dataset[mapper_id]

        filename = "input-" + str(mapper_id) + ".json"
        curr_blob = bucket.blob(filename)
        curr_blob.upload_from_string(data=json.dumps(mapper_json_data), content_type="application/json")

    
def combine_reducer_output_files():
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-reducer-output"
    bucket = storage.Bucket(storage_client, bucket_name)

    final_output = {}
    blobs_list = storage_client.list_blobs(bucket_name)
    for blob in blobs_list:
        blob_content_bytes = blob.download_as_string()
        curr_reducer_output = json.loads(blob_content_bytes.decode("utf8"))
        for key, val in curr_reducer_output.items():
            if key not in final_output:
                final_output[key] = []
            final_output[key].extend(val)
    
    final_output = dict(sorted(final_output.items()))
    return final_output

def upload_final_output_to_cloud_storage(final_output):
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-final-output"
    bucket = storage.Bucket(storage_client, bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
    blob = bucket.blob("final-output.json")
    blob.upload_from_string(data=json.dumps(final_output), content_type="application/json")