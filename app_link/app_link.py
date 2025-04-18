from design_support import design_support

from flask import Flask, request, jsonify
from sparql import query

import configparser
import json
import logging
import os
import sys
import threading
from pathlib import Path


sys.path.append(os.path.join(Path().resolve(), os.pardir, 'crawler'))

from PlanedEndPointListClass import PlanedEndPointListClass
from EndPointListClass import EndPointListClass
from CrawlingData import Crawling


logger = logging.getLogger(__name__)

app = Flask(__name__)
config = configparser.ConfigParser()
planed_end_point_list = PlanedEndPointListClass()
end_point_list = EndPointListClass()


@app.route('/v1/api/sendQuery', methods=['POST'])
def send_query():
    """GraphDBへのSPARQLリクエストをRDFへ転送する
        また、その結果を返却する

    * アプリ内の処理部がライブラリを使って分散カタログサービス内(=グラフDB)の
       データ検索機能を実行する
       データ検索の際は、以下の情報を使用する
          取得したい情報(インスタンス）の条件 : クラス情報やプロパティ、条件指定
       (これらの指定はSPARQLに準ずる)

    * ライブラリが分散カタログサービス(グラフDB)のSPARQL検索機能を呼び出す
    * アプリは分散カタログサービス(グラフDB)の検索結果ステータス、及び検索結果を取得する
    * アプリ内の処理部が検索結果から必要なデータを取得
     （または実行結果のステータスから異常が発生したことを認識)
    """
    logger.info('send_query()')
    # data = request.get_data()
    # logger.info(f'{data=}')
    #body = json.loads(data)
    if 'query' not in request.form:
        return jsonify({'message': 'Invalid missing query parameter'}), 400
    query_sql = request.form["query"]
    logger.info(f'{query_sql=}')
    # if 'query' not in body:
        # return jsonify({'message': 'Invalid missing query parameter'}), 400
    # query_sql = body['query']
    res = query(config["sparql"]["endpoint"], query_sql)

    # 設計支援システムへ送信
    try:
        design_support(config["design_support"]["url"], query_sql, res)
    except Exception:
        return jsonify(
            {'message': 'Internal error. deisgn support system'}), 500

    return res  # 200 Success


@app.route('/v1/api/getLastModify', methods=['GET'])
def get_last_modify():
    logger.info('get_last_modify()')
    return "2025-01-24T14:30:00Z"


@app.route('/v1/api/sendEndpointList', methods=['POST'])
def subscription():
    logger.info('sendEndpointList()')
    data = request.get_data()
    logger.info(f'{data=}')
    body = json.loads(data)
    if 'endpoint_list' not in body:
        return jsonify({'message': 'Invalid missing endpoint parameter'}), 400

    elist = body['endpoint_list']
    end_point_list.conbine(elist)
    logger.info(f'{end_point_list.get()=}')

    return ""  # 200 Success


def start_crawler():
    logger.info('start_crawler()')
    Crawling(end_point_list, planed_end_point_list)


if __name__ == "__main__":
    logging.basicConfig(filename='app_link.log', level=logging.INFO)

    config.read("config.ini")
    threading.Thread(target=start_crawler).start()

    app.run(port=8081, host='0.0.0.0', debug=True)
