from opensearchpy import OpenSearch

def _opensearch_kwargs():
  return {
      'host': "localhost",
      'port': 9200,
      'verify_certs': False,
      'scheme': "https",
      'username': "admin",
      'password': "admin",
  }

def opensearch_connection() -> OpenSearch:
  return OpenSearch(
    **_opensearch_kwargs()
  )