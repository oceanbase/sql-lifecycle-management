# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import urllib3
from flasgger import Swagger
from flask import Flask, render_template
from flask_cors import CORS
from flask_restful import Api
from werkzeug.exceptions import HTTPException

from api.analyzer import Analyzer
from api.monitor import *
from api.optimizer import Optimizer, Parse
from api.workbranch import Database, UserOptimization, ReadUserOptimization
from common.complex_encoder import ComplexEncoder
from common.db_query import *
from common.logger import Logger

logfile = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(logfile)

app = Flask(__name__)
app.config['SWAGGER'] = {
    'title': 'SQLess',
    'uiversion': 3,
    'version': 1
}
swagger = Swagger(app)
api = Api(app)
# Set the size limit of the requested content, which limits the size of the uploaded file
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024
app.json_encoder = ComplexEncoder

CORS(app, supports_credentials=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@app.route('/')
@app.route('/console/workbench')
@app.route('/console/optimize')
@app.route('/console/review')
@app.route('/console/analysis')
@app.route('/console/database')
@app.route('/console/monitor')
def index():
    return render_template('index.html')


@app.errorhandler(Exception)
def handle_exception(e):
    log.exception(e)
    if hasattr(e, 'code') and hasattr(e, 'description'):
        return {
            "data": {
            },
            "code": e.code,
            "success": False,
            "message": e.description
        }
    else:
        return {
            "data": {
            },
            "code": 500,
            "success": False,
            "message": str(e)
        }


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    log.exception(e)
    if e.response:
        return e.response
    else:
        return {
            "code": 500,
            "success": False,
            "message": str(e)
        }


api.add_resource(Optimizer, '/api/v1/sql/optimize')
api.add_resource(Analyzer, '/api/v1/sql/analysis')
api.add_resource(Parse, '/api/v1/sql/parse')
api.add_resource(Database, '/api/v1/user/database')
api.add_resource(UserOptimization, '/api/v1/user/optimization')
api.add_resource(ReadUserOptimization, '/api/v1/user/optimization/<tag>')
api.add_resource(TopSQL, '/api/v1/sql/topsql')
api.add_resource(SQLPlan, '/api/v1/sql/plan')
api.add_resource(SQLText, '/api/v1/sql/text')
api.add_resource(SQLDetail, '/api/v1/sql/detail')
api.add_resource(TableIndex, '/api/v1/table/index')
api.add_resource(TableStatistics, '/api/v1/table/statistics')
api.add_resource(DatabaseConnectionCheck, '/api/v1/user/database/connection-check')

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8989, threaded=True)
