import functions_framework
import requests
from google.cloud import storage
from collections import Counter
import json

CS_SERVICE_ACCOUNT_KEY_PATH = f"secrets/nirav-raje-fall2022-cloud-storage-84ed9d82b477.json"

@functions_framework.http
def handle_request(request):
    input_word = request.args.get("inputWord")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=CS_SERVICE_ACCOUNT_KEY_PATH)
    bucket_name = "mr-final-output"
    bucket = storage.Bucket(storage_client, bucket_name)

    blob = bucket.blob("final-output.json")
    blob_content_bytes = blob.download_as_string()
    final_output = json.loads(blob_content_bytes.decode("utf8"))

    docs_list = final_output[input_word]
    docs_by_freq = dict(Counter(docs_list).most_common())

    headers = {'Access-Control-Allow-Origin': '*'}
    return (docs_by_freq, 200, headers)
