# SQL-Lifecycle-Management

English | [简体中文](README_CN.md)

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
![pyversions](https://img.shields.io/badge/python%20-3.6.3%2B-blue.svg)
[![Github stars](https://img.shields.io/github/stars/oceanbase/sql-lifecycle-management?style=flat-square)](https://github.com/oceanbase/sql-lifecycle-management)
[![OpenIssue](https://img.shields.io/github/issues/oceanbase/sql-lifecycle-management)](https://github.com/oceanbase/sql-lifecycle-management/issues)

``SQL-Lifecycle-Management`` is a SQL lifecycle management product hatched from the Ant group, providing SQL closed-loop capabilities throughout all stages of develop, integration, operation and maintenance, and continuous optimization.

# Main Features

- SQL Optimization：Support SQL optimization, provide index suggestion, PMD suggestion, rewrite and other capabilities
- SQL Review：Support SQL Review of multiple ORM framework
- Slow log Analysis：Support Slow log analysis
- SQL Monitor：Provides SQL monitoring services of different engines, collects SQL, Plan, Catalog, Statistics and other data to help developers analyze SQL problems in real time

# Currently Supported Database Engine

- OceanBase(MySQL Mode)
- MySQL

# Quick Start

## Local installation

- prerequisites: recommended python == v3.8 (tested on 3.8)
It is recommended to use [conda](https://github.com/conda/conda) to create virtual environment

```shell
conda create --name slm_3.8 python=3.8
conda activate slm_3.8
```

- install

```shell
git clone https://github.com/oceanbase/sql-lifecycle-management.git

cd sql-lifecycle-management && make install
```

- meta database config

```shell
cd sql-lifecycle-management && vim db.cfg
```

- meta database schema init

```shell
mysql -h host_ip -u user_name -p
source init/init.sql
```

- visit web

```shell
cd sql-lifecycle-management && sh ./start.sh
```

visit <http://localhost:8989>

## Deploy with docker

- build

```shell
git clone https://github.com/oceanbase/sql-lifecycle-management.git
cd sql-lifecycle-management
docker build -t <your_tag> .
```

- run

```shell
docker run -itd -p 8989:8989 <image_id> /bin/bash
docker exec -it <container_id> /bin/bash
```

- meta database config

```shell
# in docker
vim db.cfg
```

- meta database init

```shell
mysql -h host_ip -u user_name -p
source init/init.sql
```

If you don't have an existing metabase. you can also install mysql service locally using Docker

```shell
docker run --name=mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=<your_password> -dit mysql:latest
```

- visit web

```shell
sh ./start.sh
```

visit <http://localhost:8989>

# Configuration

Please refer to the [Configuration Guide](https://github.com/oceanbase/sql-lifecycle-management/blob/main/CONTRIBUTING.md) for an overview on how to configure SQL-Lifecycle-Management.
part guide：

- [API Guide](https://github.com/oceanbase/sql-lifecycle-management/blob/main/docs/api-style-guide.md)
- [Code Review Guide](https://github.com/oceanbase/sql-lifecycle-management/blob/main/docs/code-review-guide.md)
- [Writing Guide](https://github.com/oceanbase/sql-lifecycle-management/blob/main/docs/writing-guide.md)

# Roadmap

- [ ] Database engine extension
  - [x] OceanBase
  - [x] MySQL
  - [ ] PostgreSQL
  - [ ] Oracle
  - [ ] TiDB
  - [ ] PolarDB
- [x] Rule precipitation
  - [x] SQL PMD
  - [x] SQL Rewrite
  - [x] Rule-Based Optimizer
- [ ] Optimizer extension
  - [x] Cost-Based Optimizer
  - [ ] Support PLSQL
  - [ ] Query-Based Workload Analysis
  - [ ] Learning-Based Optimizer
- [ ] ORM framework extension
  - [x] MyBatis
  - [ ] GORM
  - [ ] Hibernate
  - [ ] SQLAlchemy
- [ ] Multi-cloud product access
  - [ ] OceanBase Cloud
  - [ ] Aliyun RDS
  - [ ] TiCloud
- [ ] SQL optimization plugin
  - [ ] CICD product
  - [ ] SQL Console product
  - [ ] IDE plugin

# License

``SQL-Lifecycle-Management`` is licensed under [Apache - 2.0](https://github.com/oceanbase/sql-lifecycle-management/blob/main/LICENSE) License.

# Contact Us

DingTalk group: 33920014194
