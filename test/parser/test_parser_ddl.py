# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from src.parser.mysql_parser import parser


class MyTestCase(unittest.TestCase):

    def test_create_table(self):
        result = parser.parse("""CREATE TABLE `tars_user_sql_hash_level` (
  `obregion_group` varchar(32) NOT NULL,
  `tenant_group` varchar(64) NOT NULL,
  `sql_hash` varchar(128) NOT NULL,
  `pure_dbname` varchar(128) DEFAULT NULL,
  `level` varchar(4) DEFAULT NULL COMMENT 'sql级别，当前分为三级：H, M, L',
  `limit_threshold` int(11) DEFAULT NULL COMMENT '限流阈值',
  `emp_id` varchar(16) DEFAULT NULL,
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `cluster` varchar(32) DEFAULT NULL,
  `tenant_name` varchar(128) DEFAULT NULL,
  `sql_id` varchar(128) DEFAULT NULL,
  `can_limit_eleven` varchar(8) DEFAULT NULL,
  PRIMARY KEY (`obregion_group`, `tenant_group`, `sql_hash`),
  UNIQUE KEY `sql_id` (`cluster`, `tenant_name`, `sql_id`) BLOCK_SIZE 16384,
  KEY `pure_dbname` (`pure_dbname`) BLOCK_SIZE 16384) ENGINE=InnoDB DEFAULT CHARSET=utf8
  """)
        assert result['index_list'][0][0].value == '1.primary'
        assert result['index_list'][0][1] == 'PRIMARY'
        assert result['index_list'][0][2] == ['obregion_group', 'tenant_group', 'sql_hash']
        assert result['index_list'][1][0].value == '2.unique'
        assert result['index_list'][1][1] == 'sql_id'
        assert result['index_list'][1][2] == ['cluster', 'tenant_name', 'sql_id']
        assert result['index_list'][2][0].value == '3.normal'
        assert result['index_list'][2][1] == 'pure_dbname'
        assert result['index_list'][2][2] == ['pure_dbname']

    def test_create_table2(self):
        result = parser.parse("""
        CREATE TABLE `train_order_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `order_serial_no` varchar(64) NOT NULL DEFAULT '' COMMENT '订单号',
  `transaction_serial_no` varchar(64) NOT NULL DEFAULT '' COMMENT '交易流水号',
  `wisdom_travel_serial_no` varchar(160) NOT NULL DEFAULT '' COMMENT '彗星订单号',
  `electronic_serial_no` varchar(160) NOT NULL DEFAULT '' COMMENT '电子订单号',
  `train_serial_no` varchar(100) NOT NULL DEFAULT '' COMMENT '12306流水号',
  `merchant_serial_no` varchar(100) NOT NULL DEFAULT '' COMMENT '供应商订单流水号',
  `relation_order_serial_no` varchar(64) NOT NULL DEFAULT '' COMMENT '联程关联订单号,方便分片库查询',
  `connect_type` int(11) NOT NULL DEFAULT '0' COMMENT '联程类型',
  `order_source` int(11) NOT NULL DEFAULT '0' COMMENT '订单来源',
  `occupy_type` int(11) NOT NULL DEFAULT '0' COMMENT '占座模式',
  `ability_require` int(11) NOT NULL DEFAULT '0' COMMENT '能力项，位操作',
  `resign_count` int(11) NOT NULL DEFAULT '0' COMMENT '分单次数',
  `order_state` int(11) NOT NULL DEFAULT '0' COMMENT '订单状态',
  `lock_state` int(11) NOT NULL DEFAULT '0' COMMENT '锁定状态',
  `merchant_sync_state` int(11) NOT NULL DEFAULT '0' COMMENT '同步供应商状态',
  `customer_sync_state` int(11) NOT NULL DEFAULT '0' COMMENT '同步c端状态',
  `merchant_sync_count` int(11) NOT NULL DEFAULT '0' COMMENT '同步供应商供次数',
  `customer_sync_count` int(11) NOT NULL DEFAULT '0' COMMENT '同步c端次数',
  `contact_name` varchar(20) NOT NULL DEFAULT '' COMMENT '联系人姓名',
  `contact_phone` varchar(20) NOT NULL DEFAULT '' COMMENT '联系电话',
  `contact_other_phone` varchar(20) NOT NULL DEFAULT '' COMMENT '其他联系人电话',
  `contact_email` varchar(50) NOT NULL DEFAULT '' COMMENT '联系人邮箱',
  `merchant_id` varchar(50) NOT NULL DEFAULT '' COMMENT '供应商id',
  `merchant_code` varchar(50) NOT NULL DEFAULT '' COMMENT '供应商code',
  `merchant_type` int(10) NOT NULL DEFAULT '0' COMMENT '供应商类型',
  `ticket_machine_id` varchar(50) NOT NULL DEFAULT '' COMMENT '票机id',
  `window_no` varchar(50) NOT NULL DEFAULT '' COMMENT '票机窗口',
  `gmt_created` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '下单时间',
  `gmt_expired` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '过期时间',
  `gmt_canceled` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '订单取消时间',
  `gmt_booked` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '预订完成时间',
  `gmt_issue_end` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '出票截至时间',
  `gmt_arrive` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '抵达时间',
  `gmt_departure` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '出发时间',
  `departure_station` varchar(30) NOT NULL DEFAULT '' COMMENT '出发车站名称',
  `arrival_station` varchar(30) NOT NULL DEFAULT '' COMMENT '抵达车站名称',
  `train_code` varchar(10) NOT NULL DEFAULT '' COMMENT '车次号',
  `customize_type` int(10) NOT NULL DEFAULT '0' COMMENT '定制类型',
  `customize_content` varchar(100) NOT NULL DEFAULT '' COMMENT '定制内容',
  `accept_other` int(10) NOT NULL DEFAULT '0' COMMENT '是否接收其他坐席',
  `accept_no_seat` int(10) NOT NULL DEFAULT '0' COMMENT '是否接收无座',
  `back_up_seat_class` varchar(50) NOT NULL DEFAULT '' COMMENT '备选座位等级',
  `pay_type` int(10) NOT NULL DEFAULT '0' COMMENT '支付方式',
  `pay_serial_no` varchar(50) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '电子支付流水号',
  `passenger_info` varchar(300) NOT NULL DEFAULT '' COMMENT '乘客信息',
  `total_price` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '供应商出票总价',
  `total_fee` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '供应商出票总费用',
  `total_ticket_price` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '供应商总票面价',
  `remark` varchar(255) NOT NULL DEFAULT '' COMMENT '客服备注',
  `ext` varchar(2000) NOT NULL DEFAULT '' COMMENT '订单扩展属性',
  `is_delete` int(11) NOT NULL DEFAULT '0' COMMENT '是否删除(0:未删除 / 1:已删除)',
  `CreateTime` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '创建时间',
  `UpdateTime` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '修改时间',
  `env` varchar(18) NOT NULL DEFAULT 'prod' COMMENT '生产:prod，预发:stage，测试:qa，dev测试:test，集成测试:uat',
  `gmt_occupied` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '占座时间',
  `phone_verify_type` int(11) NOT NULL DEFAULT '0' COMMENT '乘客手机号核验类型(1-仅需一个码,2-需多个码,3-获取验证码或修改手机号)',
  `phone_verify_status` int(11) NOT NULL DEFAULT '0' COMMENT '乘客手机号核验状态(0-默认无需核验 1-待核验 2-需重新发送验证码 3-短信验证码已回填 4-已核验)',
  `origin_pay_serial_no` varchar(50) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '支付流水(自动获取)',
  `gmt_distribute` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '分单时间',
  `gmt_finished` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '订单完结时间',
  `related_seat` int(11) NOT NULL DEFAULT '0' COMMENT '是否连坐(0-未知，1-是,2-否)',
  `origin_electronic_serial_no` varchar(160) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '原始电子订单号',
  `is_modify` int(11) NOT NULL DEFAULT '0' COMMENT '是否修改(0:未修改 / 1:已修改)',
  `origin_train_serial_no` varchar(100) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '原始12306流水号',
  `abilities` varchar(512) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '能力项code，多个英文逗号符分割',
  `total_servicefee` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '供应商服务费',
  `gmt_notify_issue` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '通知出票时间',
  `gmt_latest_issue` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '最晚出票时间',
  `gmt_locked` datetime NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '锁单时间',
  `relation_transaction_serial_no` varchar(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '关联交易流水号',
  `merchant_name` varchar(100) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '供应商名称',
  `merchant_short_name` varchar(50) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '供应商简称',
  `inquire_type` int(11) NOT NULL DEFAULT '0' COMMENT '查询单类型0：不是，1查询单',
  `passenger_count` int(11) NOT NULL DEFAULT '0' COMMENT '乘客数量',
  `merchant_business_type` int(4) NOT NULL DEFAULT '0' COMMENT '供应商业务类型0普通，1港铁',
  `total_foreign_price` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '票面总价外币价格',
  `rebate_foreign_price` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '返点的外币价格',
  `currency` varchar(10) COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'CNY' COMMENT '币种',
  `roe` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '汇率',
  `price_check` int(4) NOT NULL DEFAULT '0' COMMENT '是否有差额,0没有，1有',
  `passenger_credit` varchar(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '乘客证件类型',
  `area_code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地区编码',
  `account_manager` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '客户经理',
  PRIMARY KEY (`id`) ,
  KEY `ix_k_order_serial_no` (`order_serial_no`),
  KEY `i_order_serial` (`order_state`,`order_serial_no`,`merchant_id`),
  KEY `i_occupy_order_state` (`occupy_type`,`order_state`,`gmt_issue_end`,`gmt_created`),
  KEY `i_occupy_order_gmt_departure` (`occupy_type`,`order_state`,`env`,`gmt_departure`),
  KEY `i_occupy_state_machine` (`occupy_type`,`order_state`,`ticket_machine_id`,`merchant_id`,`env`),
  KEY `i_order_state_gmt_created_env` (`order_state`,`gmt_created`,`env`),
  KEY `i_gmt_issue_end_gmt_issue_end` (`gmt_issue_end`,`gmt_created`),
  KEY `i_state_id_distribute` (`order_state`,`ticket_machine_id`,`merchant_id`,`env`,`gmt_distribute`,`gmt_created`),
  KEY `i_merchant_pay_serial_no` (`merchant_id`,`pay_serial_no`),
  KEY `i_ticket_machine_id_merchant_id` (`ticket_machine_id`,`merchant_id`),
  KEY `i_order_customer_UpdateTime` (`order_state`,`customer_sync_state`,`UpdateTime`),
  KEY `i_gmt_finished` (`gmt_finished`),
  KEY `i_gmt_notify_issue` (`gmt_notify_issue`),
  KEY `i_state_id_gmt_distribute` (`order_state`,`merchant_id`,`env`,`gmt_distribute`),
  KEY `i_env_gmt_distribute` (`env`,`gmt_distribute`),
  KEY `i_merchant_gmt_distribute` (`merchant_id`,`gmt_distribute`),
  KEY `i_state_merchant_gmt_distribute` (`order_state`,`merchant_id`,`gmt_distribute`),
  KEY `i_CreateTime_env` (`env`,`CreateTime`),
  KEY `i_env_gmt_created` (`env`,`gmt_created`),
  KEY `i_env_distribute_created` (`env`,`gmt_distribute`,`gmt_created`),
  KEY `i_order_state_id_env_gmt_finished` (`order_state`,`merchant_id`,`ticket_machine_id`,`env`,`gmt_finished`,`gmt_booked`),
  KEY `i_env_gmt_distribute_type` (`env`,`gmt_distribute`,`inquire_type`),
  KEY `i_state_env_distribute_merchant` (`order_state`,`env`,`gmt_distribute`,`merchant_business_type`,`gmt_created`),
  KEY `i__env_distribute_merchant` (`env`,`gmt_distribute`,`merchant_business_type`,`gmt_created`),
  KEY `i_id_distribute_gmt_created` (`ticket_machine_id`,`merchant_id`,`env`,`gmt_distribute`,`gmt_created`),
  KEY `i_type_state` (`occupy_type`,`order_state`,`ticket_machine_id`,`merchant_id`,`env`,`gmt_created`),
  KEY `i_type_state_id` (`occupy_type`,`order_state`,`ticket_machine_id`,`merchant_id`,`env`,`gmt_distribute`),
  KEY `I_machine_id_state_type_env_created` (`ticket_machine_id`,`merchant_id`,`order_state`,`occupy_type`,`env`,`gmt_created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=3529344021213246782 COMMENT='订单表:订单相关信息存储' 
    """)
        assert len(result['index_list']) == 29
        assert result['index_list'][0][0].value == '1.primary'
        assert result['index_list'][0][1] == 'PRIMARY'
        assert result['index_list'][0][2] == ['id']
        assert result['index_list'][1][0].value == '3.normal'
        assert result['index_list'][1][1] == 'ix_k_order_serial_no'
        assert result['index_list'][1][2] == ['order_serial_no']
        assert result['index_list'][25][0].value == '3.normal'
        assert result['index_list'][25][1] == 'i_id_distribute_gmt_created'
        assert result['index_list'][25][2] == ['ticket_machine_id', 'merchant_id', 'env', 'gmt_distribute',
                                               'gmt_created']


if __name__ == '__main__':
    unittest.main()
