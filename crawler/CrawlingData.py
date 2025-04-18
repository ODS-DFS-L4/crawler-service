# クローリングによる航路データの取得

# 前提： 名前空間からプレースホルダ(`/api/sparql/query`)を追記することでエンドポイントが推測できるルールが存在する

# 1. スレッドセーフなオブジェクトエンドポイントリストを監視し、要素が追加されればその要素を元に処理を開始する
#    その後、クローリング済みエンドポイントリストを作成する
# 2. 設定日時のスレッドセーフなオブジェクトから前回の更新日時を取得するとともに、現在の日時を記憶する
#   - UTC, 2025-01-24T14:30:00Z のようなオブジェクトとする
# 3. 再起処理にて、必要なエンドポイントに対してクローリングを行う
#   1. 対象のエンドポイントをクローリング済みエンドポイントリストに加える
#   2. エンドポイントにデータの更新日時を取得するAPIへリクエストを発行し、更新日時が前回の更新日時より前の場合は、再帰処理を返却する
#   3. 更新日時が前回の更新日時以後の場合はすべてのデータを取得するSPARQLクエリを発行する
#   4. 取得したトリプルをグラフDBへ登録する
#      既存に同じデータがある場合は、削除してから登録する
#   5. 取得したトリプルの目的語 (?o) のuri部分から名前空間(DNS Domain名 + マシン名）を取得する
#   6. 取得した名前空間は対象のエンドポイントのものか確認する
#       1. 別の名前空間であった場合は、エンドポイントを作成する
#       2. そのエンドポイントが、クローリング済みエンドポイントリストになければ、3.の再帰処理を行う
#       3. 3.6 または 3.6.2に該当しなければ、再帰処理を返却する
# 4. クローリング済みエンドポイントリストのエンドポイントが、クロール対象リストオブジェクトに記載されていないか確認し、記載されていないものだけ追加する
# 5. 2.の設定日時をスレッドセーフなオブジェクトに記録する
# 6. 設定ファイルから、定期更新の間隔を取得し、待機する　待機後、クロール対象リストオブジェクトからエンドポイントリストへ格納する


from SPARQLWrapper import SPARQLWrapper
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
from dataclasses import dataclass, field
from datetime import datetime
from typing import List
from pathlib import Path
import configparser
import requests
import json
import time
import requests
import os
import threading
import urllib
import copy
import logging

from PlanedEndPointListClass import PlanedEndPointListClass
from EndPointListClass import EndPointListClass
from lib_publish import PublishUtil


logger = logging.getLogger(__name__)

DEBUG = True

# 前処理

# 設定ファイルの内容を取得する
def get_config(config_path_str: str = "./config.ini") -> dict:

    # 設定ファイルから、必要な情報を読み込む
    dirname = os.path.dirname(__file__)
    config_path = Path(os.path.join(dirname, config_path_str))
    
    if not config_path.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {config_path_str}")

    # 設定ファイルの読み込み
    if config_path.exists():
        with open(config_path, 'r') as f:
            config_text = f.read()
            
        # Pythonコードとして評価可能な形式に変換
        config_dict = {}
        for line in config_text.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                config_dict[key.strip()] = value.strip('"')
            
        # 環境変数が設定されていない場合は設定ファイルの値を使用
        discovery_finder_url = config_dict.get('DISCOVERY_FINDER_URL')
        crawling_interval = config_dict.get('CRAWLING_INTERVAL')
        graphdb_read_url = config_dict.get('GRAPHDB_READ_URL')
        graphdb_insert_url = config_dict.get('GRAPHDB_INSERT_URL')
        last_updated = config_dict.get('LAST_UPDATED')
        monitor_interval = config_dict.get('MONITOR_INTERVAL')
        
        # if DEBUG:
        #     # 取得した値を出力
        #     print(f"DISCOVERY_FINDER_URL: {discovery_finder_url}")
        #     print(f"CRAWLING_INTERVAL: {crawling_interval}")
        #     print(f"GRRAPHDB_READ_URL: {graphdb_read_url}")
        #     print(f"GRAPHDB_INSERT_URL: {graphdb_insert_url}")
        #     print(f"LAST_UPDATED: {last_updated}")
            
        return {
            'DISCOVERY_FINDER_URL': discovery_finder_url,
            'CRAWLING_INTERVAL': crawling_interval,
            'GRAPHDB_READ_URL': graphdb_read_url,
            'GRAPHDB_INSERT_URL': graphdb_insert_url,
            'LAST_UPDATED': last_updated,
            'MONITOR_INTERVAL': monitor_interval
        }
        
# ホワイトリストから、クローリング対象のドメイン名一覧を取得する
def get_namespace_list():
    # 設定ファイルの読み込み
    path = "./whitelist"
    dirname = os.path.dirname(__file__)
    whitelist_path = Path(os.path.join(dirname, path))
    with open(whitelist_path, 'r') as f:
        whitelist = f.read()
    return whitelist.splitlines()
        

# urlからドメイン名を取得する
def get_domain_name(url: str) -> str:
    # TODO: httpが含まれていない場合（ドメインを直で引数に入れられている場合）は、そのまま、ドメイン名を返す
    u = urllib.parse.urlparse(url)
    return u.netloc


# ドメイン名からSPARQLクエリ用のエンドポイントを取得する
def get_endpoint(domain: str, place_holder: str = "api/sparql/query") -> str:
    return f"http://{domain}/{place_holder}"


# 航路運営者のSPARQLクエリ用エンドポイントから、更新日時を取得するエンドポイントを取得する
def get_last_updated_url(airway_operator_url: str) -> str:
    domain = get_domain_name(airway_operator_url)
    return f"http://{domain}/api/metadata/last-modified"


# グラフDBに同じトリプルが存在しないか確認
def check_triple_exist(graphdb_url: str, triple: dict) -> bool:
    # グラフDBへの接続
    sparql = SPARQLWrapper(graphdb_url)
    # tripleのsubject, predicate, objectを取得
    subject = triple['s']["value"]
    predicate = triple['p']["value"]
    obj = triple['o']["value"]
    # クエリの作成

    # 主語、述語、目的語完全一致でデータを探す
    # 参考: https://qiita.com/hodade/items/30158fba9e943132023f
    obj = obj.replace("'", "\\'").replace('\\xFF', '\\\\xFF')
    query = f"SELECT * {{ GRAPH ?g {{ <{subject}> <{predicate}> '{obj}' }}}}"
    headers = {
        'Content-Type': 'application/sparql-query',
        'Accept': 'application/sparql-query+json'
    }

    response = requests.get(graphdb_url, data=query, headers=headers)
    try:
        results = response.json()
        if results['results']['bindings']:
            # if DEBUG:
            #     print(f"{graphdb_url}\nquery: {query}")
            #     print("answer")
            #     print(results['results']['bindings'])
            return True
        else:
            return False
        
    except Exception as e:
        logger.error(f"トリプルの存在確認中にエラーが発生: {str(e)} {query}")
        return False


# グラフDBにデータを登録
def create_triple_data(endpoint: str, triple: dict, graph_url: str) -> str:
    subject = triple['s']["value"]
    predicate = triple['p']["value"]
    obj = triple['o']["value"]
    # domain = get_domain_name(endpoint)
    obj = obj.replace("'", "\\'").replace('\\xFF', '\\\\xFF')

    # TODO: objがリテラルの場合の処理を追加
    if triple['o']['type'] == 'uri':
        obj = f"<{obj}>"
    elif triple['o']['type'] == 'literal':
        obj = f"'{obj}'"
    
    # INSERT DATA {{ <http://10.0.11.264:8084:~~~> <> <>}
    query = f"INSERT DATA {{ <{subject}> <{predicate}> {obj} }}"

    # query = f"INSERT DATA {{ GRAPH <{graph_url}> {{ <{subject}> <{predicate}> '{obj}' }} }}"
    headers = {
        'Content-Type': 'application/sparql-update'
    }

    response = requests.post(endpoint, data=query, headers=headers)
    if response.status_code == 200:
        logger.info(f"INSERT {endpoint=} {query=}")
        return response.text
    else:
        return f"Failed to insert data. Status code: {response.status_code}\n{response.text}"

# 該当トリプルを削除
# TODO: 今期は実装しない
# def delete_triple(graphdb_url: str, triple: dict) -> bool:
    
#     # グラフDBへの接続
#     sparql = SPARQLWrapper(graphdb_url)
#     # tripleのsubject, predicate, objectを取得
#     subject = triple['s']["value"]
#     predicate = triple['p']["value"]
#     obj = triple['o']["value"]
#     query = f"""
#         DELETE {{
#             GRAPH ?g {{
#                 <{subject}> <{predicate}> <{obj}>
#             }}
#         }} WHERE {{
#             GRAPH ?g {{
#                 <{subject}> <{predicate}> <{obj}>
#             }}
#         }}
#     """
#     sparql.setQuery(query)
#     sparql.setReturnFormat('json')
    
#     try:
#         results = sparql.query().convert()
#         return True
        
#     except Exception as e:
#         print(f"トリプルの削除中にエラーが発生: {str(e)}")
#         return False
    

# 再起処理にて、必要なエンドポイントに対してクローリングを行う
def recursive_crawling(
        endpoint: str,
        last_updated: datetime,
        graphdb_read_url: str,
        graphdb_insert_url: str,
        crawled_domain_list: List[str],
        whitelist_path: str = "./whitelist") -> None:
    logger.info(f"recursive_crawling()")

    domain = get_domain_name(endpoint)

    # すでにクロール済みなら終了
    if domain in crawled_domain_list:
        return

    # 1. クローリング済みエンドポイントリストに追加
    crawled_domain_list.append(domain)
    logger.info(f"{crawled_domain_list=}")

    if DEBUG:
        logger.debug(f"recursive crawling: {endpoint=}, {last_updated=}, {graphdb_read_url=}, {graphdb_insert_url=}, {crawled_domain_list=}")
    
    logger.info(f"2. check endpoint update")
    # 2. エンドポイントにデータの更新日時を取得するAPIへリクエストを発行し、更新日時が前回の更新日時より前の場合は、再帰処理を返却する
    response = None
    last_updated_url = get_last_updated_url(endpoint)
    try:
        response = requests.get(last_updated_url)
    except requests.exceptions.RequestException as e:
        # 到達できないなどのエラーが発生した場合は、エラーメッセージを出力し、処理を終了する
        logger.error(f"エンドポイント {endpoint} には更新日時を取得するAPI {last_updated_url} へ到達できませんでした: {str(e)}")
        return
    
    if response is not None:
        if response.status_code == 404:
            logger.warn(f"エンドポイント {endpoint} には更新日時を取得するAPI {last_updated_url} が存在しません")
            return 
        if response.status_code == 500:
            logger.error(f"エンドポイント {endpoint} はエラーが発生しています")
            return

        # 更新日時が取得できた
        if DEBUG:
            logger.debug(f"{response.text=}")
        try:
            last_modified = response.json().get('lastModifiedAt')
        except:
            last_modified = response.text
        last_modified_dt = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%SZ')
    
        if last_modified_dt <= last_updated:
            if DEBUG:
                logger.debug(f"エンドポイント {endpoint} の更新日時 {last_modified_dt} が前回の更新日時 {last_updated} より前です")
            # TODO return

    logger.info(f"3. check last modify")
    # 3. 更新日時が前回の更新日時以後の場合はすべてのデータを取得するSPARQLクエリを発行する
    
    # .whitelist からクローリング対象のドメイン名一覧を取得し、リストオブジェクトにする
    whitelist = get_namespace_list()
        
    results = {}
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"
    try:
        url = f"{get_endpoint(domain)}"  # TODO: パス部分は固定
        if not endpoint.startswith('http'):
            url = "http://" + url
            if DEBUG:
                logger.debug(f"Get RDF data from {url}\n{query=}")
        response = requests.post(url, data=query)
        logger.info(f"requests URL {url=} {query=}")
        try:
            results = response.json()
        except:
            logger.error(f"{url=}\n{query=}\n{response.text}")
            return  # json ではない / ここにデータはなし
    except requests.exceptions.RequestException as e:
        logger.error(f"エンドポイント {endpoint} からデータを取得できませんでした: {str(e)}")
        logger.error(f"{url=}\n{query=}\n{results=}")
        raise e
    bindings = results['results']['bindings']
    if len(bindings) == 0:  # データなし
        logger.info(f"** No data **")
        return 
            
    # 取得したトリプルを一つずつグラフDBに存在しないか確認し、存在する場合は削除する
    for triple in bindings:
        # TODO: Neptuneに登録する際には、既存のデータがあっても問題なく処理が実行される
        # if check_triple_exist(graphdb_read_url, triple):
        #     # if DEBUG:
        #         # print(f"triple exist.\n{triple=}")
        #     pass

        # TODO: 削除処理

        # 一つのトリプル毎にINSERT処理を行う
        if not create_triple_data(graphdb_insert_url, triple, endpoint):
            logger.error(f"トリプルの登録に失敗しました: {endpoint=}, {triple=}")

        # 再帰処理をするかジャッジする
        # 5. 取得したトリプルの目的語 (?o) のuri部分から名前空間(DNS Domain名 + マシン名）を取得する
        if triple['o']['type'] != 'uri':  # uriでない場合はスキップ
            continue

        o_namespace = get_domain_name(triple['o']['value'])
        namespace_url = get_endpoint(o_namespace)
        logger.info(f"{o_namespace=} {whitelist=}")
        
        # 今回はクローリング対象か確認するホワイトリストに、クローリング対象の名前空間が含まれているか確認する
        if o_namespace in whitelist:
            logger.info(f"In whitelist")
            # Nmaespaceがクローリング済みエンドポイントリストに含まれていないか確認する
            if o_namespace not in crawled_domain_list:
                logger.info(f"In {crawled_domain_list=}")
                # 6. クローリング対象の名前空間の場合は、再帰処理を行う
                # testだけホワイトリストものだけトリプルを追加
                # create_triple_data(graphdb_insert_url, triple, endpoint)
                recursive_crawling(
                    namespace_url, last_updated,
                    graphdb_read_url, graphdb_insert_url, crawled_domain_list)
            else:
                if DEBUG:
                    logger.debug(f"クローリング済みの名前空間: {o_namespace}, {crawled_domain_list=}")


# クローリング処理
def crawling_data(endpoint_list: List, last_updated: datetime) -> None:
    logger.info(f"crawling_data()")
    
    # endpoint_listの被りがないようにする
    endpoint_list = list(set(endpoint_list))
    
    # 設定ファイルの読み込み
    config = get_config()
    
    # Neptune接続設定
    graphdb_read_url = config['GRAPHDB_READ_URL']
    graphdb_insert_url = config['GRAPHDB_INSERT_URL']
    
    crawled_domain_list = []
    
    # クローリング対象リストのエンドポイントが、クロール済みエンドポイントリストにないか確認し、ないものだけ追加する
    for endpoint in endpoint_list:
        # 3.の再帰処理を行う
        recursive_crawling(
            endpoint, last_updated, graphdb_read_url, graphdb_insert_url, crawled_domain_list)
    return


# エンドポイント監視クラス
class EndPointMonitor(threading.Thread):
    def __init__(self, endpoint_list_obj: EndPointListClass, planed_endpoint_list_obj: PlanedEndPointListClass, last_updated: datetime):
        super().__init__()
        self.endpoint_list_obj = endpoint_list_obj
        self.planed_endpoint_list_obj = planed_endpoint_list_obj
        self.last_updated = last_updated
        self._stop_event = threading.Event()
        
    def stop(self):
        """スレッドを停止するメソッド"""
        self._stop_event.set()
        
    def run(self):
        """エンドポイントリストを監視し続け、要素が追加されればその要素を元に処理を開始する"""
        while not self._stop_event.is_set():
            try:
                # エンドポイントリストの取得
                endpoint_list = self.endpoint_list_obj.get()
                logger.info(f"{endpoint_list=}")
                
                if len(endpoint_list) > 0:
                    # クローリング処理
                    crawling_data(endpoint_list, self.last_updated)
                                        
                    # 設定日時を更新
                    self.last_updated = datetime.now()
                    logger.info(f"{self.last_updated=}")

                    # 各ドメインごとにデータをパブリッシュ
                    for endpoint in endpoint_list:
                        domain = get_domain_name(endpoint)
                        topic = domain
                        msg = "updated"
                        # データをパブリッシュ
                        pubobj = PublishUtil( "localhost", 1883)
                        pubobj.connect()
                        status = pubobj.publish(topic, msg)
                        if DEBUG:
                            if status == 0:
                                logger.info(f"publish success: {topic} {msg}")
                            else:
                                logger.warn(f"publish failed: {topic} {msg}")
                        pubobj.disconnect()
                    
                    logger.info(f"{self.endpoint_list_obj.get()=}")
                    for eobj in self.endpoint_list_obj.get():
                        logger.info(f"append{eobj=}")
                        if eobj not in self.planed_endpoint_list_obj.get():
                            self.planed_endpoint_list_obj.append(eobj)
                    logger.info(f"{self.planed_endpoint_list_obj.get()=}")

                    # 処理を終えたエンドポイントをクリア
                    self.endpoint_list_obj.clear()

                # 設定ファイルから、定期更新の間隔を取得し、待機する
                config = get_config()
                monitor_interval = int(config['MONITOR_INTERVAL'])
                time.sleep(monitor_interval)
                
            except Exception as e:
                logger.error(f"EndpointListMonitor error: {str(e)}")
                time.sleep(60)  # エラー時は1分待機してから再試行



# クローリング間隔処理クラス
class CrawlingScheduler(threading.Thread):
    def __init__(self, endpoint_list_obj: EndPointListClass, planed_endpoint_list_obj: PlanedEndPointListClass, last_updated: datetime):
        super().__init__()
        self.endpoint_list_obj = endpoint_list_obj
        self.planed_endpoint_list_obj = planed_endpoint_list_obj
        self.last_updated = last_updated
        self._stop_event = threading.Event()

    def stop(self):
        """スレッドを停止するメソッド"""
        self._stop_event.set()
        
    def run(self):
        """クローリング間隔がすぎると、エンドポイントリストに、予約されいるエンドポイントを追加する"""
        # 設定ファイルからクローリング間隔を取得
        config = get_config()
        crawling_interval_str = config['CRAWLING_INTERVAL']
        crawling_interval = int(crawling_interval_str)        
        
        # 待機前の更新日時オブジェクトを別のメモリー空間に記憶
        last_updated_tmp = self.last_updated

        # クローリング間隔分待機
        time.sleep(crawling_interval)

        while not self._stop_event.is_set():
            try:
                logger.info(f"planed_endpoint_list_obj: {self.planed_endpoint_list_obj.get()}")
                # クローリング間隔を再取得
                config = get_config()
                crawling_interval_str = config['CRAWLING_INTERVAL']

                # # 待機中にエンドポイントリストが更新されていないか確認
                # if last_updated_tmp == self.last_updated:
                # クロール対象リストオブジェクトからエンドポイントリストへ格納する
                logger.info(f"{self.planed_endpoint_list_obj.get()=}")
                self.endpoint_list_obj.conbine(self.planed_endpoint_list_obj.get())
                # 待機時間をそのまま設定
                remaining_time = int(crawling_interval_str)
                # 更新日時を記憶
                last_updated_tmp = datetime.now()
                
                # # 待機中にエンドポイントリストが更新されている場合は、残り待機時間をして待機
                # else:
                #     # 後どれだけ待機するかを計算
                #     elapsed_time = datetime.now() - last_updated_tmp
                #     remaining_time = int(crawling_interval_str) - elapsed_time.total_seconds()
                    
                # 待機
                logger.info(f"remaining_time: {remaining_time}")
                time.sleep(remaining_time)
                
                
            except Exception as e:
                logger.error(f"CrawlingScheduler error: {str(e)}")
                time.sleep(60)


class Crawling():
    def __init__(self, endpoint_list_obj: EndPointListClass, planed_endpoint_list: PlanedEndPointListClass):
        # 設定日時(UTC)で、初期値は1990-01-01T00:00:00Zとする
        last_updated = datetime.strptime('1990-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
            
        # 「エンドポイントリストを監視し続け、要素が追加されればその要素を元に処理を開始する」処理と
        # 「クローリング間隔がすぎると、エンドポイントリストに、予約されいるエンドポイントを追加する」処理の
        # 二つのスレッドを建てる
        # それぞれの処理は、無限ループする
        
        # エンドポイント監視スレッド
        endpoint_monitor = EndPointMonitor(endpoint_list_obj, planed_endpoint_list, last_updated)
        # クローリング間隔処理スレッド
        crawling_scheduler = CrawlingScheduler(endpoint_list_obj, planed_endpoint_list, last_updated)
        
        try:
            # スレッドの開始
            endpoint_monitor.start()
            crawling_scheduler.start()
            
            # スレッドの終了待ち
            endpoint_monitor.join()
            crawling_scheduler.join()
            
        except KeyboardInterrupt:
            logger.info("Shutting down threads...")
            # スレッドの停止
            endpoint_monitor.stop()
            crawling_scheduler.stop()
            # スレッドの終了待ち
            endpoint_monitor.join()
            crawling_scheduler.join()
            logger.info("All threads are stopped.")
            
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            # スレッドの停止
            endpoint_monitor.stop()
            crawling_scheduler.stop()
            # スレッドの終了待ち
            endpoint_monitor.join()
            crawling_scheduler.join()
            logger.error("All threads are stopped.")
