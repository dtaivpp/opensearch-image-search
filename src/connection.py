from opensearchpy import OpenSearch
import requests

def _opensearch_kwargs():
  return {
      'host': "localhost",
      'port': 9200,
      'verify_certs': False,
      'scheme': "https",
      'username': "admin",
      'password': "admin",
  }

def opensearch_connection(value) -> OpenSearch:
  return OpenSearch(
    **_opensearch_kwargs()
  )


def os_request(method, endpoint: str, data=""):
  full_url = f"https://admin:admin@localhost:9200/{endpoint}"
  return requests.request(
    method,
    full_url, 
    verify=False, 
    data=data,
    headers={
      'Content-Type': 'application/json'
    }
  )


if __name__=="__main__":
  response = os_request('GET', '_cat/health')
  print(response)