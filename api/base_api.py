# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import json

from flask import Flask, Response, abort, jsonify
from flask_restful import Resource, Api
from flask_restful.reqparse import Argument

app = Flask(__name__)
app.config["BUNDLE_ERRORS"] = True

api = Api(app)

# csrf check
referrer_white_list = [
    'https://sqless.opentrscdn.com',
    'http://sqless.opentrs.com',
    'https://sqless.opentrs.com',
    'https://sqless.opentrs.cn',
    'http://sqless.opentrs.cn',
]


class BaseAPI(Resource):
    def __init__(self, *args, **kwargs):
        super(BaseAPI, self).__init__(*args, **kwargs)
        self.user_id = 'sqless'

    def construct_success_response_entity(self, data={}, success=True, total_count=0, message='', code=200):
        return jsonify({
            "data": data,
            "code": code,
            "success": success,
            "message": message,
            "totalCount": total_count
        })

    def construct_error_response_entity(self, message, code=500):
        return jsonify({
            "code": code,
            "success": False,
            "message": message
        })


class APIArgument(Argument):
    def __init__(self, *args, **kwargs):
        super(APIArgument, self).__init__(*args, **kwargs)

    def handle_validation_error(self, error, bundle_errors):
        help_str = "(%s) " % self.help if self.help else ""
        msg = "[%s]: %s%s" % (self.name, help_str, str(error))
        res = Response(
            json.dumps({"message": msg, "code": 400, "success": False}),
            mimetype="application/json",
            status=400,
        )
        return abort(res)
