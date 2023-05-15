# SQL-Lifecycle-Management

[English](README.md) | 简体中文

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
![pyversions](https://img.shields.io/badge/python%20-3.6.3%2B-blue.svg)
[![Github stars](https://img.shields.io/github/stars/oceanbase/sql-lifecycle-management?style=flat-square)](https://github.com/oceanbase/sql-lifecycle-management)
[![OpenIssue](https://img.shields.io/github/issues/oceanbase/sql-lifecycle-management)](https://github.com/oceanbase/sql-lifecycle-management/issues)

``SQL-Lifecycle-Management``是一款从蚂蚁业务场景孵化的SQL生命周期管理产品，提供了贯穿研发、集成、运维和持续优化各个阶段的SQL闭环能力。

# 核心能力
- SQL优化：支持SQL优化，提供索引建议、PMD建议、Rewrite重写等能力
- SQL Review：支持多种ORM框架SQL Review能力
- 慢查分析：支持Slow log分析功能
- SQL监控：提供不同引擎的SQL监控服务，采集SQL、Plan、元数据、统计信息等数据帮助开发者实时分析SQL问题

# 当前支持的数据库引擎
- OceanBase(MySQL Mode)
- MySQL

# 快速入门
- 执行环境: 推荐版本python >= v3.6.3
- 依赖安装
```shell
git clone https://github.com/oceanbase/sql-lifecycle-management.git

cd sql-lifecycle-management && make install
```
- 数据库初始化
```shell
# 填写本地元数据库链接方式
cd sql-lifecycle-management && vim db.cfg
# 本地元数据库初始化
mysql -h host_ip -u user_name -p
source init/init.sql
```
- 访问页面
```shell
cd sql-lifecycle-management && sh ./start.sh
```
访问页面http://localhost:8989

# 开发参与
欢迎开发者参与我们的开源社区，为我们的产品开发和维护做出贡献。
了解如何参与开发：[贡献指南](https://github.com/oceanbase/sql-lifecycle-management/blob/main/CONTRIBUTING.md) 
部分规范：
- [API规范](https://github.com/oceanbase/sql-lifecycle-management/blob/main/docs/api-style-guide.md) 
- [Code Review规范](https://github.com/oceanbase/sql-lifecycle-management/blob/main/docs/code-review-guide.md) 
- [拼写规范](https://github.com/oceanbase/sql-lifecycle-management/blob/main/docs/writing-guide.md) 

# Roadmap
- [ ] 数据库引擎扩展
  - [x] OceanBase
  - [x] MySQL
  - [ ] PostgreSQL
  - [ ] Oracle
  - [ ] TiDB
  - [ ] PolarDB
- [x] 规则场景沉淀
  - [x] SQL PMD
  - [x] SQL Rewrite
  - [x] Rule-Based Optimizer
- [ ] 优化器扩展
  - [x] Cost-Based Optimizer
  - [ ] 支持 PLSQL
  - [ ] Query-Based Workload Analysis
  - [ ] Learning-Based Optimizer
- [ ] ORM框架扩展
  - [x] MyBatis
  - [ ] GORM
  - [ ] Hibernate
  - [ ] SQLAlchemy
- [ ] 多云产品接入
  - [ ] OceanBase Cloud
  - [ ] Aliyun RDS
  - [ ] TiCloud
- [ ] SQL优化插件
  - [ ] CICD产品
  - [ ] SQL Console产品
  - [ ] IDE插件

# 许可证
``SQL-Lifecycle-Management`` 使用 [Apache - 2.0](https://github.com/oceanbase/sql-lifecycle-management/blob/main/LICENSE) 许可证。

# 联系我们
钉钉群: 33920014194

