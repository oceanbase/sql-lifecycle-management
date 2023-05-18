CREATE TABLE `database_asset` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `user_id` varchar(32) NOT NULL COMMENT '用户ID',
  `database_alias` varchar(32) NOT NULL COMMENT '唯一别名',
  `database_name` varchar(32) NOT NULL COMMENT '数据库名',
  `engine` varchar(32) NOT NULL COMMENT '数据库引擎',
  `version` varchar(32) NOT NULL COMMENT '数据库版本',
  `platform` varchar(32) NOT NULL COMMENT '云平台',
  PRIMARY KEY (`db_id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '数据库资产元数据';


CREATE TABLE `user_optimization` (
  `tag` varchar(32) NOT NULL COMMENT 'tag',
  `user_id` varchar(32) NOT NULL COMMENT '用户名',
  `engine` varchar(32) NOT NULL COMMENT '数据库引擎',
  `type` varchar(32) NOT NULL COMMENT '诊断逻辑',
  `status` varchar(32) NOT NULL COMMENT '诊断状态',
  `database_alias` varchar(32) NOT NULL COMMENT '唯一别名',
  `is_read` int(2) NOT NULL COMMENT '是否已读',
  `sql_text_list` text NOT NULL COMMENT 'SQL文本',
  `optimization_detail` text NOT NULL COMMENT '诊断详情',
  `gmt_create` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `deal_time` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '结束时间',
  PRIMARY KEY (`tag`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '用户优化结果报告';


CREATE TABLE `monitor_database` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `approve_type` varchar(32) NOT NULL COMMENT '数据采集的授权方式：pull-中心式拉取、push-db端推送、manual-人工上传',
  `approve_scope` varchar(64) NOT NULL COMMENT '数据收集的授权范围，如果是pull自动拉取，可选值包含：sql/plan/schema/statistics',
  `host_ip` varchar(32) COMMENT 'DB服务器地址，pull授权必填',
  `host_port` int(8) COMMENT 'DB服务器端口，pull授权必填',
  `user_name` varchar(32) COMMENT 'DB连接用户名，pull授权必填，ob需要拼接集群租户',
  `password` varchar(32)  COMMENT 'DB连接密码，pull授权选填，空表示使用sqless默认提供的密码',
  `gmt_create` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `gmt_modify` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`db_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '数据库监控配置';


CREATE TABLE `schedule_queue` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `run_type` varchar(20) NOT NULL COMMENT '调度类型',
  `object_info` varchar(128) NOT NULL COMMENT '调度对象',
  `check_point` varchar(128) NOT NULL COMMENT '位点信息',
  `gmt_create` timestamp NOT NULL COMMENT '创建时间',
  `gmt_modify` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`db_id`, `run_type`, `object_info`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '调度队列位点信息';


CREATE TABLE `monitor_sql_text` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `sql_id` varchar(128) NOT NULL COMMENT 'SQL唯一ID',
  `sql_type` varchar(32) DEFAULT NULL COMMENT 'SQL类型',
  `sql_text` text NOT NULL COMMENT '带参的原始SQL文本',
  `user_name` varchar(80) DEFAULT NULL COMMENT '用户名',
  `statement` text NULL DEFAULT NULL COMMENT '参数化后的SQL文本',
  `table_list` varchar(400) NULL DEFAULT NULL COMMENT 'SQL请求的Table列表',
  `gmt_create` timestamp NOT NULL COMMENT '写入时间',
  `gmt_modify` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`db_id`, `sql_id`),
  KEY `idx_sql_id` (`sql_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'SQL文本';


CREATE TABLE `monitor_sql_auidt_oceanbase` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `sql_id` varchar(128) NOT NULL COMMENT 'SQL唯一ID',
  `request_time` datetime NOT NULL COMMENT '请求时间',
  `sql_type` bigint(20) DEFAULT NULL COMMENT 'SQL类型',
  `executions` bigint(20) NOT NULL COMMENT '执行次数',
  `elapsed_time` bigint(20) DEFAULT NULL COMMENT '响应耗时',
  `cpu_time` bigint(20) DEFAULT NULL COMMENT 'CPU耗时',
  `queue_time` bigint(20) DEFAULT NULL COMMENT '队列耗时',
  `getplan_time` bigint(20) DEFAULT NULL COMMENT '硬解析耗时',
  `netwait_time` bigint(20) DEFAULT NULL COMMENT '网络耗时',
  `iowait_time` bigint(20) DEFAULT NULL COMMENT 'IO耗时',
  `total_wait_time` bigint(20) DEFAULT NULL COMMENT '内部等待耗时',
  `logical_reads` bigint(20) DEFAULT NULL COMMENT '逻辑读',
  `return_rows` bigint(20) DEFAULT NULL COMMENT '返回行数',
  `affected_rows` bigint(20) DEFAULT NULL COMMENT '影响行数',
  `fail_times` bigint(20) DEFAULT NULL COMMENT '失败次数',
  `retry_cnt` bigint(20) DEFAULT NULL COMMENT '重试次数',
  `rpc_count` bigint(20) DEFAULT NULL COMMENT 'RPC次数',
  `remote_plans` bigint(20) DEFAULT NULL COMMENT '远程请求次数',
  `miss_plans` bigint(20) DEFAULT NULL COMMENT '未命中计划次数',
  `cluster` varchar(32) NOT NULL COMMENT 'OB集群',
  `tenant_name` varchar(64) NOT NULL COMMENT 'OB租户',
  `svr_ip` varchar(64) NOT NULL COMMENT 'OB服务器IP',
  `client_ip` varchar(40) DEFAULT NULL COMMENT '客户端IP',
  `user_name` varchar(80) DEFAULT NULL COMMENT '用户名',
  `plan_hash` bigint(20) unsigned DEFAULT NULL COMMENT '执行计划ID',
  `table_scan` tinyint(4) DEFAULT NULL COMMENT '全表扫描标记',
  `gmt_create` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`db_id`, `request_time`, `sql_id`, `svr_ip`),
  KEY `idx_sql_id` (`sql_id`, `request_time`),
  KEY `idx_request_time` (`request_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'SQL流水-OceanBase';


CREATE TABLE `monitor_sql_auidt_mysql` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `sql_id` varchar(128) NOT NULL COMMENT 'SQL唯一ID',
  `request_time` datetime NOT NULL COMMENT '请求时间',
  `sql_type` bigint(20) DEFAULT NULL COMMENT 'SQL类型',
  `executions` bigint(20) NOT NULL COMMENT '执行次数',
  `query_time` bigint(20) DEFAULT NULL COMMENT '响应耗时',
  `row_sent` bigint(20) DEFAULT NULL COMMENT '返回行数',
  `row_affected` bigint(20) DEFAULT NULL COMMENT '影响行数',
  `row_examined` bigint(20) DEFAULT NULL COMMENT '扫描行数',
  `client_ip` varchar(40) DEFAULT NULL COMMENT '客户端IP',
  `user_name` varchar(80) DEFAULT NULL COMMENT '用户名',
  PRIMARY KEY (`db_id`, `request_time`, `sql_id`),
  KEY `idx_sql_id` (`sql_id`, `request_time`),
  KEY `idx_request_time` (`request_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'SQL流水-MySQL';


CREATE TABLE `monitor_sql_plan_oceanbase` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `sql_id` varchar(128) NOT NULL COMMENT 'SQL唯一ID',
  `tenant_name` varchar(64) NOT NULL COMMENT 'OB租户',
  `svr_ip` varchar(64) NOT NULL COMMENT 'OB服务器IP',
  `sql_type` varchar(32) DEFAULT NULL COMMENT 'SQL类型',
  `plan_hash` bigint(20) unsigned NOT NULL COMMENT '计划ID',
  `plan_type` bigint(20) DEFAULT NULL COMMENT '计划类型',
  `query_sql` text DEFAULT NULL COMMENT 'SQL文本',
  `first_load_time` timestamp(6) NULL DEFAULT NULL COMMENT '计划生成时间',
  `merged_version` bigint(20) DEFAULT NULL COMMENT '合并版本',
  `last_active_time` timestamp(6) NULL DEFAULT NULL COMMENT '最近活跃时间',
  `avg_exe_usec` bigint(20) DEFAULT NULL COMMENT '平均响应耗时',
  `hit_count` bigint(20) DEFAULT NULL COMMENT '命中次数',
  `table_scan` tinyint(4) DEFAULT NULL COMMENT '全表扫描标记',
  `plan_info` text NOT NULL COMMENT '计划信息',
  `plan_detail` text DEFAULT NULL COMMENT '计划详情-包含extend内容',
  `outline_hash` varchar(32) DEFAULT NULL COMMENT '绑定hash，根据data内容生成',
  `outline_id` bigint(20) DEFAULT NULL COMMENT '绑定id',
  `outline_data`text DEFAULT NULL COMMENT '绑定内容',
  `outline_time` timestamp(6) NULL DEFAULT NULL COMMENT '绑定时间',
  `gmt_create` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `gmt_modify` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`db_id`, `sql_id`, `plan_hash`),
  KEY `sql_id_time` (`sql_id`, `first_load_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'SQL计划-OceanBase';


CREATE TABLE `meta_table_index` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `table_name` varchar(64) NOT NULL COMMENT '表名',
  `index_name` varchar(64) NOT NULL COMMENT '索引名',
  `index_type` varchar(32) DEFAULT NULL COMMENT '索引类型',
  `index_status` varchar(32) DEFAULT NULL COMMENT '索引状态',
  `column_list` varchar(2048) DEFAULT NULL COMMENT '字段列表，多字段用逗号分隔',
  `gmt_create` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `gmt_modify` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`db_id`, `table_name`, `index_name`),
  KEY `idx_table_name` (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '表索引信息';


CREATE TABLE `meta_table_statistics` (
  `db_id` varchar(64) NOT NULL COMMENT '数据库唯一ID，包含用户id+唯一别名',
  `table_name` varchar(64) NOT NULL COMMENT '表名',
  `column_name` varchar(128) NOT NULL COMMENT '字段名',
  `ndv_count` bigint(20) NOT NULL COMMENT '字段NDV',
  `table_rows` bigint(20) NOT NULL COMMENT '表行数',
  `min_value` varchar(128) DEFAULT NULL COMMENT '字段最小值',
  `max_value` varchar(128) DEFAULT NULL COMMENT '字段最大值',
  `gmt_create` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `gmt_modify` timestamp(6) NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`db_id`, `table_name`, `column_name`),
  KEY `idx_request_time` (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '表统计信息';



