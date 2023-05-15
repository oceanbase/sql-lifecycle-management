# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


import logging
import logging.config
import os
import time

clevel_default = logging.ERROR
flevel_default = logging.INFO

standard_format = '[%(asctime)s %(levelname)s %(process)d] - [%(filename)s:%(lineno)d] ' \
                  + '%(module)s.%(funcName)s.%(lineno)d: %(message)s'
simple_format = '[%(asctime)s %(levelname)s] %(message)s'


class Logger(object):

    def __init__(
            self,
            logfile='',
            clevel=clevel_default,
            flevel=flevel_default):
        # init file name
        self.log_path = os.getcwd().split('sqless')[0] + 'sqless/logs/'
        if not logfile:
            self.rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
            self.logfile = self.log_path + self.rq + '.log'
        else:
            self.logfile = self.log_path + logfile
        logging_dict = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {'format': standard_format},
                'simple': {'format': simple_format},
            },
            'filters': {},
            'handlers': {
                'console': {
                    'level': clevel,
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple'
                },
                'default': {
                    'level': flevel,
                    'formatter': 'simple',
                    'filename': self.logfile,
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'when': 'D',
                    'interval': 1,
                    'backupCount': 3,
                    'encoding': 'utf-8'
                },
            },
            'loggers': {
                '': {
                    'handlers': ['default', 'console'],
                    'level': flevel_default,
                    'propagate': False,
                },
            },
        }
        logging.config.dictConfig(logging_dict)
        self.logger = logging.getLogger()

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warn(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def exception(self, message):
        self.logger.exception(message)
