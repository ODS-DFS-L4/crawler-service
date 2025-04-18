# Crawler Service

このリポジトリは、ドローン航路のあり方に係る調査・研究として、ドローン航路システムに係るCrawler Serviceのサンプルを公開しています。

## 目次

- [システム概要](#システム概要)
- [構築と設定](#構築と設定)
- [起動方法](#起動方法)
- [ライセンス](#ライセンス)
- [免責事項](#免責事項)

## システム概要

Crawler Serviceはドローン航路システムが開示するデータをクロールする機能です。  
ドローン航路システムと連携するシステムがクロールするためのサンプルです。

### システム構成

本ライブラリは分散カタログサービスとしての二つの機能を提供します。
1. Web API
    1. クロールしたデータをアプリ側が検索・取得する機能・アプリへの変更を通知する機能
    2. `/app_link` ディレクトリ配下が該当
2. クローラ
    1. ドローン航路システムからのクロール機能
    2. `crawler` ディレクトリ配下が該当

## 構築と設定

### ファイル配置

Crawler Serviceサーバー内の任意のディレクトリに本リポジトリのファイル一式を展開します。

### Web API設定

以下ファイルを編集し、Web APIに必要な情報を設定します。  
`app_link/config.ini`
```ini
[sparql]            # クロールしたデータを参照するためのグラフDBのエンドポイント
endpoint=https://ro.graphdb.example.com:8182/sparql

[design_support]    # アプリ利用者がクロールしたデータを参照するときのクエリおよび参照結果を受け取る、設計支援システムのエンドポイント
url=http://design_support:5000/catalog/v1/query-response
```

### クローラ設定

以下ファイルを編集し、クローラに必要な情報を設定します。
`crawler/config.ini`
```ini
# ディスカバリーファインダーのURL
DISCOVERY_FINDER_URL="http://localhost:8080"
# ディスカバリーサービスのドメイン名
DISCOVERY_SERVICE_DOMAIN="example3.com"
# クローリング間隔（秒）
CRAWLING_INTERVAL=3600
# 分散カタログサービスのグラフDBのエンドポイント
GRAPHDB_READ_URL="https://ro.graphdb.example.com:8182/sparql"
GRAPHDB_INSERT_URL="https://graphdb.example.com:8182/sparql"
# 本サービスが行ったGraphDBの前回アップデート日時 (将来機能で利用)
LAST_UPDATED="20250101T01:01:01"
# クローリングを監視する間隔（秒）
MONITOR_INTERVAL=60
```

以下のファイルを編集し、クローリング対象のドメインを記載します。
`crawler/whitelist`  

例:
```ini
10.0.2.194:8084
10.0.11.246:8084
airway.example.com:8890
```

### 必要なパッケージのインストール

1. Mosquitto MQTT サーバーをインストール
   ```sh
   $ sudo apt-get install mosquitto
   $ sudo apt-get install mosquitto-clients
   ```
   mosquittoが稼働しているか以下のコマンドで確認します。
   ```sh
   $ systemctl status mosquitto
   ```
2. 必要なPython パッケージをインストール
   ```sh
   $ pip install ./requirements.txt
   ```

## 起動方法

Pythonコードを実行します。
```sh
$ cd app_link
$ python app_link.py
```

## ライセンス

- 本リポジトリはMITライセンスで提供されています。
- ソースコードおよび関連ドキュメントの著作権はIntentExchange株式会社に帰属します。

## 免責事項
- 本リポジトリの内容は予告なく変更・削除する可能性があります。
- 本リポジトリの利用により生じた損失及び損害等について、いかなる責任も負わないものとします。
