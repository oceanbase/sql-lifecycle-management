# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


class UnLoginException(Exception):
    code = 10001
    description = "not login"


class CsrfException(Exception):
    code = 10002
    description = "csrf check failed"


class FileIsNoneException(Exception):
    code = 10003
    description = "File is None"


class FileTypeNotSupportsException(Exception):
    code = 10004
    description = "The file type suffix only supports .xml and .log"
