import json
import logging
import requests


logger = logging.getLogger(__name__)


def design_support(url: str, query: str, response: str):
    """設計支援サービスへのAPI発行
    """
    logger.info(f"design_support {url}")

    data = {
        "query": query,
        "response": response
    }
    logger.info(f"{data=}")
    ret = requests.post(
        f"{url}",
        data=json.dumps(data),
        headers={"Content-Type": "application/json"})
    if ret.status_code != 200:
        err = "設計支援システムへの情報通知に失敗しました"
        logger.error(f"ERROR {ret.text}")
        raise ValueError(err)
