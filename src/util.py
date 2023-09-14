import os
import time
import logging
from logging.handlers import RotatingFileHandler
import json
from datetime import datetime
from opensearchpy import OpenSearch
from connection import opensearch_connection

logger = logging.getLogger('dataset-loader')
_OS_CLIENT = None


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
  os_client = get_os_client()
  for batch in data:
    logger.debug("Performing Bulk Insert")
    try:
      response = os_client.bulk(batch)
      logger.debug(response)
    except Exception as e:
      logger.exception(e)


def backoff(current_backoff, max_backoff=360) -> int:
  """Calculates exponential backoff"""
  if not (current_backoff**2) > max_backoff:
    return current_backoff**2

  return max_backoff


def initialize_opensearch_client():
  """Instantiates an instance of the OpenSearch client for reuse"""
  global _OS_CLIENT  # pylint: disable=[W0603]
  _OS_CLIENT = opensearch_connection()


def get_os_client():
  """Returns Already Instantiated OpenSearch Client"""
  if _OS_CLIENT is None:
    initialize_opensearch_client()
  return _OS_CLIENT


def init_logging(_logger: logging.Logger, log_level):
  """Initialize the logging"""
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
  log_dir = './log'

  if not os.path.isdir(log_dir):
    try:
      os.makedirs(log_dir)
    except:
      raise PermissionError("Couldnt create %s", log_dir)

  # File Handler
  file_handle = RotatingFileHandler(
      log_dir + "/dataset_loader.log", backupCount=15)
  file_handle.setLevel(logging.DEBUG)
  file_handle.setFormatter(formatter)

  # Console Handler
  console_handle = logging.StreamHandler()
  console_handle.setLevel(logging.DEBUG)
  console_handle.setFormatter(formatter)

  # Add Handlers
  _logger.addHandler(file_handle)
  _logger.addHandler(console_handle)

  # Set Level
  _logger.setLevel(logging.getLevelName(log_level))
