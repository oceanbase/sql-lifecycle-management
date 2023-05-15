# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""
from flask import request
from flask_restful import reqparse
from werkzeug.utils import secure_filename

from api.api_utils import ApiUtils
from api.base_api import APIArgument, BaseAPI
from api.exceptions import *
from common.db_query import *
from common.enum import AnalysisFileTypeEunm, OptimizationTypeEunm
from common.security_check import allowed_file
from consume.mybatis_sqlmap_parse import MybatisXmlParser
from consume.mysql_slowlog_parse import SlowQueryParser
from metadata.metadata_utils import MetaDataUtils

UPLOAD_FOLDER = './save'


class Analyzer(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(Analyzer, self).__init__(*args, **kwargs)
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('fileType', required=True, choices=['xml', 'slow_log'],
                            help="The fileType only supports xml/slow_log")
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('schemaSQL')
        parser.add_argument('catalogJson')
        parser.add_argument('ormFrame')
        args = parser.parse_args()
        self.file = request.files['file']
        self.file_type = args['fileType']
        self.db_alias = args['databaseAlias']
        catalog = args.get('catalogJson', '{}')
        try:
            self.catalog_json = json.loads(catalog) if catalog else None
        except Exception as e:
            self.catalog_json = ''
            self.schema_sql = args.get('catalogJson', '')

    def post(self):
        """
            sql analysis
            ---
            tags:
              - Analyzer
            parameters:
                - in: body
                  name: file
                  type: file
                  description: 文件
                  required: true
                - in: body
                  name: fileType
                  type: string
                  enum: ['xml', 'slow_log']
                  description: 文件类型
                  required: true
                - in: body
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: body
                  name: catalogJson
                  type: string
                  description: catalog信息
                  example:
                   "columns": [
                     {"schema":"luli1","table":"adbase_ad_word","name":"id","type":"bigint(20) unsigned","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"gmt_create","type":"datetime","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"gmt_modified","type":"datetime","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"word","type":"varchar(256)","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"status","type":"tinyint(4)","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"source","type":"tinyint(4)","nullable":true},
                     {"schema":"luli1","table":"adbase_ad_word","name":"order_index","type":"bigint(20)","nullable":true}
                    ]
                   "indexes": [
                    {"schema":"luli1","table":"adbase_ad_word","name":"PRIMARY","column":"id","cardinality":0,"unique":true},
                    {"schema":"luli1","table":"adbase_ad_word","name":"uk_word","column":"word","cardinality":0,"unique":true},
                    {"schema":"luli1","table":"adbase_ad_word","name":"idx_word","column":"word","cardinality":0,"unique":false},
                    {"schema":"luli1","table":"adbase_ad_word","name":"idx_word_status","column":"word","cardinality":0,"unique":false},
                    {"schema":"luli1","table":"adbase_ad_word","name":"idx_word_status","column":"status","cardinality":0,"unique":false}
                   ]
                   "tables": [
                    {"schema":"luli1","table":"adbase_ad_word","rows":0,"engine":"InnoDB"}
                   ]
                   "version": "5.7.36"
                  required: false
                - in: body
                  name: ormFrame
                  type: string
                  description: orm框架
                  required: false
            responses:
                200:
                   description: analysis result
        """
        if self.file is None:
            raise FileIsNoneException()

        if not allowed_file(self.file.filename):
            raise FileTypeNotSupportsException()

        schema_sql = self.schema_sql
        catalog_json = self.catalog_json
        file = self.file
        catalog_object = ''
        if catalog_json:
            catalog_object = MetaDataUtils.json_to_catalog(catalog_json) if catalog_json else None
        elif schema_sql:
            catalog_object = MetaDataUtils.schema_sql_to_catalog_index(schema_sql)

        review_detail_list = []
        total_grade = 0
        review_summary_set = set()
        optimization_type = None
        first_sql_text = ''

        if self.file_type == AnalysisFileTypeEunm.XML.value:
            xml_parse = MybatisXmlParser()
            # remove illegal content from filename
            file_name = self.user_id + '_' + str(calendar.timegm(time.gmtime())) + '_' + secure_filename(
                file.filename)
            file_path = os.path.join(UPLOAD_FOLDER, file_name)
            file.save(file_path)

            sql_list = xml_parse.parse_mybatis_xml_file(file_path)

            for per_sql in sql_list:
                sql_text = per_sql['sql_text']
                if not sql_text:
                    continue
                if not first_sql_text:
                    first_sql_text = sql_text
                review_detail_list.append(ApiUtils.get_xml_log_details(sql_text, catalog_object))
            optimization_type = OptimizationTypeEunm.REVIEW.value
        elif self.file_type == AnalysisFileTypeEunm.SLOW_LOG.value:
            db_info = get_db_info(self.user_id, self.db_alias)
            file_name = str(self.user_id) + '_' + str(calendar.timegm(time.gmtime())) + '_' + secure_filename(
                file.filename)
            file_path = os.path.join(UPLOAD_FOLDER, file_name)
            file.save(file_path)
            if db_info:
                version = db_info['version']
                sort = 'total_time'
                query_parser = SlowQueryParser(file_path, version, sort)
                sql_list = query_parser.parser_from_log()
                for per_sql in sql_list:
                    sql_text = per_sql['sql_text']
                    if not sql_text:
                        continue
                    if not first_sql_text:
                        first_sql_text = sql_text
                    review_detail_list.append(ApiUtils.get_xml_log_details(sql_text, catalog_object))
            optimization_type = OptimizationTypeEunm.ANALYSIS.value

        if review_detail_list:
            for review_detail in review_detail_list:
                total_grade += review_detail['grade']
                if review_detail['report']:
                    report = review_detail['report']
                    if report['indexOptimizeationRecommendations']:
                        for _index_recommend in report['indexOptimizeationRecommendations']:
                            review_summary_set.add(_index_recommend['index_recommendation'])
                    if report['developmentSpecificationRecommendations']:
                        for _spec_recommend in report['developmentSpecificationRecommendations']:
                            review_summary_set.add(_spec_recommend['pmdRule'])

            total_grade = int(total_grade / len(review_detail_list))

        data = {
            "grade": total_grade,
            "reviewSummary": list(review_summary_set),
            "reviewDetail": review_detail_list
        }

        insert_user_optimization(self.user_id, self.db_alias, first_sql_text, data,
                                 optimization_type)

        return self.construct_success_response_entity(data=data)
