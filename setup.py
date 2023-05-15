# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

# This is the main testing file which can be run as `python setup.py test`

from setuptools import setup

setup(
    name='SQL-Lifecycle-Management',
    version='1.0',
    description="a SQL lifecycle management product hatched from the Ant group, providing SQL closed-loop capabilities throughout all stages of develop, integration, operation and maintenance, and continuous optimization.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/oceanbase/sql-lifecycle-management",
    license='Apache-2.0',
    python_requires='>=3.6.3',
    install_requires=[
        'sqlalchemy==1.3.1',
        'pymysql==0.9.3',
        'DBUtils==1.3',
        'flask==2.0.1',
        'requests==2.21.0',
        'flask_restful==0.3.9',
        'Flask-API==1.0',
        'werkzeug==2.0.2',
        'unittest-xml-reporting==3.0.4',
        'future==0.16.0',
        'flask-cors==3.0.10',
        'fire==0.3.1',
        'lxml==4.6.2',
        'tqdm==4.53.0',
        'flask_request_id==0.1',
        'Pycrypto==2.6.1',
        'pytest',
        'simplejson==3.18.4',
        'schedule',
        'flasgger'
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache-2.0 License",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
