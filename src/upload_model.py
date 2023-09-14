import warnings
from os import getenv
from opensearchpy import OpenSearch
from opensearch_py_ml.ml_commons import MLCommonClient
import dotenv 

dotenv.load_dotenv()

OS_HOST=getenv("OPENSEARCH_HOST")
OS_PW=getenv("OPENSEARCH_PW")
OS_UN=getenv("OPENSEARCH_USER")

client = OpenSearch(
    hosts=OS_HOST, 
    http_auth=(OS_UN, OS_PW),
    verify_certs=False,
)


ml_client = MLCommonClient(client)


#model_id = "20aw24kBmpBndJ_5IBk7"
model_id = ml_client.register_pretrained_model(
    model_name = "huggingface/sentence-transformers/all-MiniLM-L12-v2", 
    model_version = "1.0.1", 
    model_format = "TORCH_SCRIPT", 
    deploy_model=True, 
    wait_until_deployed=True)

load_model_output = ml_client.deploy_model(model_id=model_id)

""" pipeline_config={
  "description": "An example neural search pipeline",
  "processors" : [
    {
      "text_embedding": {
        "model_id": model_id,
        "field_map": {
          "content": "content_embedding"
        }
      }
    }
  ]
} """

""" client.ingest.put_pipeline(
    id="TEST_PIPE", 
    body=pipeline_config
) """

index_settings={
    "settings": {
        "index.knn": True,
        "default_pipeline": "TEST_PIPE"
    },
    "mappings": {
        "properties": {
            "content_embedding": {
                "type": "knn_vector",
                "dimension": 384,
                "method": {
                  "name": "hnsw",
                  "space_type": "l2",
                  "engine": "nmslib"
                }
            },
            "content": { 
                "type": "text"            
            },
        }
    }
}

result = client.indices.create(
    index="nlp-index", 
    body=index_settings
)

