import logging
import requests


logger = logging.getLogger(__name__)


def query(endpoint_url: str, sql: str):
    """ GraphDB(AWS neptune) への query
    """
    logger.info(f"query {endpoint_url}")

    return_format = 'json'
    headers = {
        'Content-Type': 'application/sparql',
        'Accept': f'application/sparql-results+{return_format}'
    }
    response = requests.get(
        endpoint_url, params={'query': sql}, headers=headers)
    if response.status_code == 200:
        logger.info(f"{response.text}")
        return response.json()

    # TODO: エラー処理
    logger.error(f"ERROR {response.text=}")
    return response.text
