# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from chardet.universaldetector import UniversalDetector


def get_encoding(file: str):
    """
    get file encoding
    :param file:
    :return: dict {'encoding': 'utf-8', 'confidence': 0.99, 'language': ''}
    """
    txt = open(file, "rb")
    detector = UniversalDetector()
    for line in txt.readlines():
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    txt.close()
    char_encoding = detector.result
    if char_encoding['encoding'] in ['Windows-1254', 'gb2312', 'gbk']:
        read_encoding = 'gbk'
    else:
        read_encoding = 'utf-8'
    return read_encoding
