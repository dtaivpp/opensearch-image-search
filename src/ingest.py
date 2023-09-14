from util import to_opensearch
import json
import dotenv 
from os import getenv
from opensearchpy import OpenSearch
from pprint import pprint
from opensearchpy.helpers import bulk

dotenv.load_dotenv()

OS_HOST=getenv("OPENSEARCH_HOST")
OS_PW=getenv("OPENSEARCH_PW")
OS_UN=getenv("OPENSEARCH_USER")

client = OpenSearch(
    hosts=OS_HOST, 
    http_auth=(OS_UN, OS_PW),
    verify_certs=False,
)

def to_ndjson(_list: list) -> list:
  """Translates list of dicts to ndjson string"""
  batch = []
  for chunk in _divide_chunks(_list, 200):
    batch.append('\n'.join([json.dumps(item, default=str) for item in chunk]))
  return batch


def _divide_chunks(data: list, number: int):
  for i in range(0, len(data), number):
    yield data[i:i + number]


def to_opensearch(data: list):
  """Sends data to OpenSearch

  Parameters:
  os_client: OpenSearch Client
  data: List of ndjson strings
  """
  os_client = client
  for batch in data:
    response = os_client.bulk(batch)

data=None

with open("data/documentation-index.json", mode="r") as f:
  data=json.load(f)
  pprint(data)

all_data = []
for row in data:
  all_data.append({"index": {"_index": "nlp-index", "_id": row['url']}})
  all_data.append(row)

to_opensearch(to_ndjson(all_data))