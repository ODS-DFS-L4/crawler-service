from flask import Flask, request

import configparser
import logging


logger = logging.getLogger(__name__)

app = Flask(__name__)
config = configparser.ConfigParser()


@app.route('/catalog/v1/query-response', methods=['POST'])
def send_query():
    print(request.get_json())
    return request.form


if __name__ == "__main__":
    app.run(port=8080, host='0.0.0.0', debug=True)
