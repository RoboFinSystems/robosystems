import os
import requests
from requests.exceptions import RequestException
from retrying import retry

from robosystems.logger import logger
from robosystems.config import env, ExternalServicesConfig

OPENFIGI_CONFIG = ExternalServicesConfig.OPENFIGI_CONFIG
BASE = OPENFIGI_CONFIG["base_url"]
MAPPING = BASE + OPENFIGI_CONFIG["mapping_endpoint"]


# Create conditional decorator that uses fast retries in test environment
def conditional_retry(func):
  """Apply retry decorator with test-friendly settings."""
  # Check if we're in a test environment
  if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("TESTING"):
    # Use very fast retries for tests
    return retry(
      stop_max_attempt_number=OPENFIGI_CONFIG["retry_attempts"],
      wait_fixed=1,  # 1ms fixed wait
    )(func)
  return retry(
    stop_max_attempt_number=OPENFIGI_CONFIG["retry_attempts"],
    wait_random_min=env.OPENFIGI_RETRY_MIN_WAIT,
    wait_random_max=env.OPENFIGI_RETRY_MAX_WAIT,
  )(func)


class OpenFIGIClient:
  @conditional_retry
  def get_figi(self, payload):
    openfigi_headers = OPENFIGI_CONFIG["headers"].copy()
    api_key = env.OPENFIGI_API_KEY
    if api_key:
      openfigi_headers["X-OPENFIGI-APIKEY"] = api_key
    try:
      response = requests.post(url=MAPPING, headers=openfigi_headers, json=payload)

      if response.status_code == 429:
        raise RequestException("OpenFIGI API Rate Limit Exceeded")
      elif response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")

      return response.json()
    except Exception:
      logger.error("Waiting for OpenFIGI API limits to reset")
      raise RequestException("OpenFIGI retry required")
