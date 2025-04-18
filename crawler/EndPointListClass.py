"""EndPointList の管理モジュール."""
import logging
import threading


logger = logging.getLogger(__name__)

class EndPointListClass:
    endpoint_list = []

    def __init__(self):
        self.lock = threading.Lock()

    def get(self):
        logger.info(f"{self.endpoint_list=}")
        return self.endpoint_list

    def append(self, endpoint: str):
        with self.lock:
            self.endpoint_list.append(endpoint)

    def conbine(self, endpoint_list: list):
        with self.lock:
            self.endpoint_list.extend(endpoint_list)

    def clear(self):
        with self.lock:
            self.endpoint_list = []
