# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import os
import re
import sys
import uuid
from copy import deepcopy
from typing import Dict, Tuple, Optional

from lxml import etree
from lxml.etree import tostring, ElementTree

from common.logger import Logger
from consume.file_parse_common import get_encoding

logfile = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(logfile)

# Mybatis
MYBATIS_SINGLE_VALUE_CONDITION = (
    "isEqual", "isNotEqual", "isEqualTo", "isGreaterThan", "isGreaterEqual", "isLessEqual", "isGreaterThanOrEqualTo",
    "isLessThan", "isLessThanOrEqualTo", "isLike", "isLikeCaseInsensitive", "isNotEqualTo", "isNotLike",
    "isNotLikeCaseInsensitive")
MYBATIS_UNARY_CONDITIONAL = (
    "isPropertyAvailable", "isNotPropertyAvailable", "isNull", "isNotNull", "isEmpty", "isNotEmpty")
QUERY_TYPE = ("select", "update", "insert", "delete")
BLACKLIST_KEYWORD = (
    "wherecase", "groupbycase", "criteria", "criterion", "__cdt__", "if(", "$tableSuffix$", "${dynamicSelect}",
    "over(partition", "onduplicatekeyupdate")
GENERATED_KEYWORD = ("@mbg", "generated", "auto-generation")

# Mybatis regex
MYBATIS3_VAR_DYNAMIC_RE0 = re.compile(r"\$\{([\w\.\,=\s:\(\)]+?)\}\[\]([.\w]+)?", flags=re.IGNORECASE)  # ${}[].xx
MYBATIS3_VAR_DYNAMIC_RE01 = re.compile(r"\$\{([\w\.\,=\s:\(\)]+?)\}", flags=re.IGNORECASE)  # ${}
MYBATIS3_VAR_DYNAMIC_RE02 = re.compile(r"\$([\w\.\,=\s:\(\)]+?)\$", flags=re.IGNORECASE)  # $xx$
MYBATIS3_VAR_DYNAMIC_RE1 = re.compile(r"\#([\w\.\,=\s:\(\)\"]+?)\[\]([.\w]+)?\#", flags=re.IGNORECASE)  # #xxx[].xx#
MYBATIS3_VAR_DYNAMIC_RE2 = re.compile(r"\#([\w\.\,=\s:\(\)\"]+?)\#", flags=re.IGNORECASE)  # #xxx#
MYBATIS_VAR_RE = re.compile(r"\#([\{\}\w\[\]\.\,=\s:\(\)\"]+?)\#", flags=re.IGNORECASE)
MYBATIS3_VAR_RE = re.compile(r"\#\{([\{\}\w\[\]\.\,=\s:\(\)\"]+?)\}", flags=re.IGNORECASE)
SORT_METADATA_RE = re.compile(r"\$(\S*):METADATA\$", flags=re.IGNORECASE)
PAGEDIRECTION_SQLKEYWORD = re.compile(r"\$(\S*):SQLKEYWORD\$", flags=re.IGNORECASE)
REVERT_COMMENT_RE = re.compile(r"(-- .*?;)", flags=re.IGNORECASE)
REMOVE_COMMENT_RE = re.compile(r"(-- ?\S*)", flags=re.IGNORECASE)
FIX_DYNAMIC_TABLENAME = re.compile(r"\s+from\s+\"(\S*)\"", flags=re.IGNORECASE)
FIX_DYNAMIC_WHERE_RE = re.compile(r"\s+where\s+((and)|(or))", flags=re.IGNORECASE | re.MULTILINE)
FIX_DYNAMIC_AND_RE = re.compile(r"\(\s*and ", flags=re.IGNORECASE)
FIX_DYNAMIC_OR_RE = re.compile(r"\(\s*or ", flags=re.IGNORECASE)
FIX_DYNAMIC_ORDER_BY_RE = re.compile(r"order\s+by\s+(.*)", flags=re.IGNORECASE)
FIX_NESTED_PREFIXOVERRIDE_RE = re.compile(r"\(\s*,", flags=re.IGNORECASE | re.MULTILINE)
FIX_AND_AND_RE = re.compile(r"((and)|(,))\s+((and)|(,))", flags=re.IGNORECASE | re.MULTILINE)
LIMIT_RE = re.compile(r'\s+limit\s+(".*")', flags=re.IGNORECASE)
OFFSET_RE = re.compile(r'\s+offset\s+(".*")', flags=re.IGNORECASE)
LIMIT_OFFSET_RE = re.compile(r'\s+limit\s+(--.*;)?\s*(\".*?\")\s*,\s*(--.*;)?\W*(\".*?\")',
                             flags=re.IGNORECASE | re.MULTILINE)
LIMIT_OFFSET_RE2 = re.compile(r'\s+limit\s+(--.*;)?\s*(.*?)\s*,\s*(--.*;)?\W*(\".*?\")',
                              flags=re.IGNORECASE | re.MULTILINE)
GBK_FIX_RE = re.compile(r'\s+using\s+gbk', flags=re.IGNORECASE)
ORACLE_FOR_UPDATE_NOWAIT_FIX_RE = re.compile(r'(for\s+update(\s+(no)?[_]?wait)?(\s+[\d.])?)', flags=re.IGNORECASE)
ORACLE_UPSERT_FIX_RE = re.compile(r'\s+upsert ', flags=re.IGNORECASE)
ORACLE_DELETE_FROM_ORDER_BY_FIX_RE = re.compile(r'(ORDER BY.*)$', flags=re.IGNORECASE | re.MULTILINE)
PREPEND_SET_FIX_RE = re.compile(r'\s+set\s+,', flags=re.IGNORECASE)
HINT_EXT_RE = re.compile(r'(/\*\+ index.*?\*/)', flags=re.IGNORECASE)
COUNT_DOT_FIX_RE = re.compile(r'(count\(\S+.\*\))', flags=re.IGNORECASE)
PARSE_UID_RE = re.compile(r' -- parse_uid:\w{32};')


def get_include_define(tree: ElementTree()) -> Dict[str, str]:
    """
    parse xml include defined
    :param tree: xml tree
    :return: include_sql_dict: include dict: {"refid":"str"}

    include exsample:
        <sql id="AffBizEvent.columns">
        id, event_name, description, event_type, unique_id
        </sql>
        <operation name="queryAllEvent" multiplicity="many">
            <sql>
                SELECT
                <include refid="AffBizEvent.columns"/>
                FROM aff_biz_event
                WHERE
                    is_deleted = 0 AND status = 1
                    AND l1_domain_code = ?
            </sql>
        </operation>
    """
    include_sql_dict = dict()
    # get namespace
    namespace = tree.getroot().get("namespace")
    if not namespace:
        namespace = tree.getroot().get("sqlname")
    # get include sql
    sqls = tree.getroot().findall("sql")
    if sqls:
        for sql in sqls:
            if sql.get("id"):
                # namespace needs to be added before refid, because some include references across namespaces
                include_sql_dict[f"{namespace}.{sql.get('id')}"] = sql
    return include_sql_dict


def replace_or_rm_el_from_tree(elem: ElementTree, replace_el: ElementTree = None):
    """
    replace or remove element from etree
    :param elem: target etree
    :param replace_el: replace element，default None，None means remove
    :return: None
    """
    parent = elem.getparent()
    el_index = parent.index(elem)
    last_index_of_inserted_replace_el = el_index
    parent.remove(elem)
    elem_tail_inserted = False
    if el_index == 0:
        parent.text = "" if not parent.text else parent.text
        parent.text += replace_el.text if replace_el is not None else ""
    else:
        parent.getchildren()[el_index - 1].tail += replace_el.text if replace_el is not None else ""
    if replace_el is not None:
        for child in reversed(replace_el.getchildren()):
            last_child = deepcopy(child)
            parent.insert(el_index, last_child)
            last_index_of_inserted_replace_el += 1
        if last_index_of_inserted_replace_el >= 1:
            parent.getchildren()[last_index_of_inserted_replace_el - 1].tail += replace_el.tail or ""
    if el_index == 0 and el_index == last_index_of_inserted_replace_el:
        parent.text += elem.tail or ""
    elif not elem_tail_inserted and last_index_of_inserted_replace_el >= 1:
        # append tail of include tag to last inserted el, because include tag usually do not have text, text is not added here
        parent.getchildren()[last_index_of_inserted_replace_el - 1].tail += elem.tail or ""


def strip_parse_uid(sql: str):
    """
    strip parse_uid from sql text
    :param sql: sql text
    :return: sql text by stripped
    """
    if ("parse_uid") in sql:
        return re.sub(PARSE_UID_RE, "", sql).strip()
    else:
        return sql.strip()


def strip_parse_uid_from_dict(target_dict: Dict[str, str]):
    """
    strip parse_uid from dict
    :param target_dict: sql dict
    :return: sql dict by stripped
    """
    for k, v in target_dict.items():
        for item in v:
            if isinstance(item, dict) and item.get("value"):
                item["value"] = strip_parse_uid(item.get("value"))
    return target_dict


def remove_comment_inplace(target_element: ElementTree):
    """
    remove comments include xml and sql
    :param target_element:
    :return: None
    """
    comments = target_element.xpath('//comment()')
    for c in comments:
        parent = c.getparent()
        p = c.getparent()
        if p is None:
            del c
            continue
        el_index = parent.index(c)
        if el_index == 0:
            parent.text += c.tail or ""
        elif parent.getchildren()[el_index - 1].tail:
            parent.getchildren()[el_index - 1].tail += c.tail or ""
        else:
            parent.getchildren()[el_index - 1].tail = c.tail or ""
        p.remove(c)
    for query in target_element.getiterator():
        if query.text and "--" in query.text:
            query.text = re.sub(REMOVE_COMMENT_RE, "\n", query.text)
        if query.tail and "--" in query.tail:
            query.tail = re.sub(REMOVE_COMMENT_RE, "\n", query.tail)


def parse_eltree_to_text(tree: ElementTree, query_type):
    """
    get sql text from parse xml tree
    :param tree: xml tree
    :param query_type: sql type
    :return: res: sql text
    :return: mybatis_info:
    """
    res = ""
    stack = []
    tail_stack = []
    mybatis_info = {}
    first_ele_it_self = True
    for elem in tree.getiterator():
        # determine whether exist include or selectkey unprocessed
        if elem.tag.lower() == "include":
            print(f"[Error] recursive include found {elem.attrib}")
            raise NotImplementedError("[Error] recursive include found")
        if elem.tag.lower() == "selectkey":
            res += elem.tail or ""
            continue

        should_not_append_tail = False
        if len(elem) > 0 and not first_ele_it_self:
            stack.append(len(elem))
            tail_stack.append(elem.tail or "")
            should_not_append_tail = True
        if not stack:
            should_not_append_tail = False

        first_ele_it_self = False

        if elem.tag in ("where", "set"):
            if elem.tag == "set":
                if elem.getchildren() and elem.getchildren()[-1].text.strip().endswith(",") and not elem.getchildren()[
                    -1].tail.strip():
                    elem.getchildren()[-1].text \
                        = elem.getchildren()[-1].text.strip()[:-1] + "\n"  # remove last comma in dynamic sql
                if elem.text.strip().endswith(","):
                    elem.text = elem.text.strip()[:-1] + "\n"  # remove last comma in dynamic sql
                if not elem.text.strip().endswith(",") and len(elem.getchildren()) > 0:
                    elem.text += ","

            res += f" {elem.tag.upper()} \n"
            res += elem.text or ""
        elif elem.tag == query_type:
            res += elem.text or ""
        elif elem.tag in [*MYBATIS_UNARY_CONDITIONAL, *MYBATIS_SINGLE_VALUE_CONDITION, "if", "when", "dynamic"]:
            res += (elem.get("prepend", "") + " " + elem.get("open", "") + " " + (elem.text or ""))

            # handle multiple ele in elem, prepend need to be apply to each of them
            if elem.get("prepend", "").strip().upper() in ["AND", "OR"] and len(elem.getchildren()) > 1:
                first_el_prefend = True
                for el in elem.getchildren():
                    if not first_el_prefend:
                        el.text = elem.get("prepend", "") + " " + el.text
                    if first_el_prefend is True:
                        first_el_prefend = False
            if elem.text:
                uid = uuid.uuid4().hex
                cmt = {**dict(elem.attrib), **{"tag": elem.tag}}
                mybatis_info[uid] = cmt
                if elem.text.strip():
                    res += f" -- parse_uid:{uid};\n"
            if elem.get("close"):
                if elem.getchildren():
                    if elem.getchildren()[-1].tail:
                        elem.getchildren()[-1].tail += " " + elem.get("close") + " "
                    else:
                        elem.getchildren()[-1].tail = " " + elem.get("close") + " "
                else:
                    res += elem.get("close") + " "
        elif elem.tag in ("iterate", "foreach"):
            res += (elem.get("prepend", "") + " " + elem.get("open", "") + " " + f"{elem.text or ''}" + " ")
            if len(elem) > 0 and not first_ele_it_self and stack and stack[-1]:
                tail_stack[-1] = elem.get("close", "") + tail_stack[-1]
            else:
                res += elem.get("close", "")
        elif elem.tag in ("trim",):
            if elem.get("prefixOverrides"):
                for fix in elem.get("prefixOverrides").split("|"):
                    if elem.getchildren() and elem.getchildren()[0].text.strip().startswith(fix.strip()):
                        elem.getchildren()[0].text = elem.getchildren()[0].text.strip() \
                            .replace(fix, " ", 1)  # equals rreplace
                    elif not elem.getchildren() and elem.text.strip().startswith(fix.strip()):
                        elem.text = elem.text.strip().replace(fix, " ", 1)
            res += elem.get("prefix", "") + " "
            if elem.get("suffixOverrides"):
                for fix in elem.get("suffixOverrides").split("|"):
                    # trim_target = ""
                    fix = fix.strip()
                    if elem.getchildren():
                        tail = elem.getchildren()[-1].tail
                        text = elem.getchildren()[-1].text
                    else:
                        tail = elem.tail
                        text = elem.text

                    if tail and tail.strip().endswith(fix):
                        # trim last element tail
                        tail = tail[:-len(fix)]
                    elif tail:
                        # has tail but not end with fix
                        pass
                    elif text and text.strip().endswith(fix):
                        # trim last element text
                        text = text[:-len(fix)]

            res += (elem.text or "") + " "
            if elem.get("suffix"):
                if elem.getchildren():
                    if elem.getchildren()[-1].tail:
                        elem.getchildren()[-1].tail += elem.get("suffix")
                    else:
                        elem.getchildren()[-1].tail = elem.get("suffix")
                else:
                    res += elem.get("suffix", "") + " "
        elif elem.tag == "include":
            res += (elem.get("refid"))
        else:
            res += elem.text or ""

        if not should_not_append_tail:
            res += elem.tail or ""

        while stack and stack[-1] == 0:
            stack.pop()
            res += tail_stack.pop()

        if stack and stack[-1] > 0:
            stack[-1] -= 1

    return res, mybatis_info


def match_include_reference(tree: ElementTree, namespace, include_sql_dict):
    """
    quote the included place from the etree and replace the quote with the match
    :param tree: target etree
    :param namespace:
    :param include_sql_dict:
    :return: None

    temporarily does not support include replacement of property
    example:
    <sql id="affevent.abc">
        ${abc},username
    </sql>

    <select id="query_abc" resultType="User_abc">
        select
        <include refid="affevent.abc">
            <property name="abc" value="id"/>
        </include>
        from t_user_abc
    </select>
    """
    for elem in tree.getiterator():
        if elem.tag == "include":
            id_attrib = elem.get("id") or elem.get("refid")
            # namespace needs to be added before refid, because some include references across namespaces
            if "." not in id_attrib:
                id_attrib = f"{namespace}.{id_attrib}"
            elif f"{namespace}.{id_attrib}" in include_sql_dict:
                id_attrib = f"{namespace}.{id_attrib}"
            # replace the referenced include
            if id_attrib in include_sql_dict:
                replace_el = include_sql_dict.get(id_attrib)
                replace_or_rm_el_from_tree(elem, replace_el)
            else:
                # because only the processing of a single xml file is currently supported,
                # all sqlmaps will not be scanned,
                # so the include references across namespaces cannot be processed
                log.error(f"refid not found include tag, maybe reference to other namespace: {elem.attrib}")
                raise NotImplementedError("refid not found include tag")
        if elem.tag == "selectkey":
            replace_or_rm_el_from_tree(elem)


def convert_sqltext_mybatis_syntax(sql_str):
    """
    convert mybatis with sql syntax
    :param sql_str
    :return: sql_str
    :return: error_msg

    dynamic where exsample：
    <select id="getClusterByTenantList" resultClass="string">
        select cluster from ob_topsql_meta
        <dynamic prepend="where">
            <isNotEmpty prepend="AND" property="tenantList">
                tenant_name in
                <iterate property="tenantList" open="(" close=")" conjunction=",">
                    #tenantList[]#
                </iterate>
            </isNotEmpty>
        </dynamic>
        GROUP BY cluster
    </select>

	field value parameterization:
    <select id="selectColumns" resultType="entity.Column">
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = #{tableName}
        AND table_schema = #{dbName}
    </select>
    """
    error_msg = ''

    sql_str = sql_str.replace("\n", " ")
    sql_str = sql_str.replace("(+)", " ")
    sql_str = re.sub(REVERT_COMMENT_RE, " \g<1>\n", sql_str)
    sql_str = re.sub(SORT_METADATA_RE, " $\g<1>$ ", sql_str)
    sql_str = re.sub(PAGEDIRECTION_SQLKEYWORD, " $\g<1>$ ", sql_str)
    order_by = re.search(FIX_DYNAMIC_ORDER_BY_RE, sql_str)
    if order_by and order_by.group(1):
        group_1 = order_by.group(1).lower()
        after_group_by = ""
        for sp in (")", "limit", "where"):
            parts = group_1.split(sp)
            if len(parts) > 1:
                group_1 = parts[0]
                after_group_by += "".join(group_1[1:])
        if ("$" in group_1
            or "#" in group_1
            or "{" in group_1) \
                and not group_1.endswith(",") \
                and not ("desc" in group_1 or "asc" in group_1):
            sql_str = re.sub(FIX_DYNAMIC_ORDER_BY_RE,
                             f" order by sort_column desc -- in xml it's not desc; {after_group_by}", sql_str)

    # dynamic sql types that need to be skipped
    skip_relist = [MYBATIS3_VAR_DYNAMIC_RE0, MYBATIS3_VAR_DYNAMIC_RE01, MYBATIS3_VAR_DYNAMIC_RE02,
                   MYBATIS3_VAR_DYNAMIC_RE1, MYBATIS3_VAR_DYNAMIC_RE2, FIX_DYNAMIC_TABLENAME]
    for per_re in skip_relist:
        m = re.search(per_re, sql_str)
        if m:
            error_msg = 'Unsupport dynamic sql type'
            return '', error_msg

    sql_str = re.sub(MYBATIS3_VAR_RE, " ?", sql_str)
    sql_str = re.sub(MYBATIS_VAR_RE, " ?", sql_str)
    sql_str = re.sub(FIX_DYNAMIC_WHERE_RE, " where ", sql_str)
    sql_str = re.sub(FIX_DYNAMIC_AND_RE, " (", sql_str)
    sql_str = re.sub(FIX_DYNAMIC_OR_RE, " (", sql_str)
    sql_str = re.sub(FIX_NESTED_PREFIXOVERRIDE_RE, "( -- fix for analyse\n", sql_str)
    sql_str = re.sub(FIX_AND_AND_RE, " and -- fix for analyse\n", sql_str)
    sql_str = re.sub(COUNT_DOT_FIX_RE, " count(*) -- fix for analyse\n", sql_str)
    sql_str = re.sub(LIMIT_OFFSET_RE, "\n limit 1", sql_str)
    sql_str = re.sub(LIMIT_OFFSET_RE2, "\n limit 1\n", sql_str)
    sql_str = re.sub(LIMIT_RE, "\n limit 1 \n", sql_str)
    sql_str = re.sub(OFFSET_RE, "\n offset 1 \n", sql_str)
    sql_str = re.sub(GBK_FIX_RE, " using utf8 -- in xml it's gbk;\n", sql_str)
    sql_str = re.sub(PREPEND_SET_FIX_RE, " set ", sql_str)

    return sql_str, error_msg


def single_query_parse(namespace, include_sql_dict, query, query_type):
    """
    parse xml tree and process mybatis grammar to get sql text
    :param namespace
    :param include_sql_dict
    :param query
    :param query_type
    :return: sql_text
    :return: error_msg
    """
    match_include_reference(query, namespace, include_sql_dict)
    remove_comment_inplace(query)
    text, mybatis_info = parse_eltree_to_text(query, query_type)
    sql_text, error_msg = convert_sqltext_mybatis_syntax(text)
    return sql_text, error_msg


def parse_mybatis_xml_tree(tree: ElementTree(), include_sql_dict):
    """
    parse xml tree to get sql list
    :param tree:
    :param include_sql_dict:
    :return: sql_list:
    """
    # get namespace
    namespace = tree.getroot().get("namespace")
    if not namespace:
        namespace = tree.getroot().get("sqlname")
    sql_list = []
    for query_type in QUERY_TYPE:
        for query in tree.findall(query_type):
            query_info = {"query_info": {**dict(query.attrib), **{"query_type": str(query.tag)}}}
            sql_id = query_info.get('query_info', {}).get('id', 'unknow')
            try:
                sql_text, error_msg = single_query_parse(namespace, include_sql_dict, query, query_type)
                formatted_xml = tostring(query, pretty_print=True, encoding="utf-8").decode()
                res_json = {
                    "line": f"{query.sourceline}",  # orig
                    "xml": str(formatted_xml),  # include (one layer), the formatted mapper xml
                    "sql_id": sql_id,
                    "sql_text": sql_text,
                    "error_msg": error_msg
                }
                sql_list.append(res_json)
            except (NotImplementedError, ValueError) as e:
                pass
            except Exception as e:
                log.exception(e)
                raise e
    return sql_list


class MybatisXmlFile(object):
    """Class implementing a parser for the Mybatis sqlmap xml file

    The MybatisXmlFile-class implements a parser for Mybatis sqlmap xml file and
    has the following capabilities:
    - Parse sqlmap xml file entries
    - Parse include reference define and sql operation
    - Parse mybatis-x-mapper define and DML operation(select/insert/update/delete)
    """

    def __init__(self, file_name: str):
        self.file_name = file_name

    def check_xml_valid(self) -> bool:
        """
        exclude unsupported xml formats
        :return: bool
        """
        if "template" in self.file_name.lower() \
                or "-INF" in self.file_name \
                or "pom.xml" in self.file_name \
                or (("da" not in self.file_name.lower()) and ("map" not in self.file_name.lower())):
            return False
        return True

    def load_xml_file(self) -> Tuple[bool, Optional[ElementTree], str]:
        """
        load the xml file and parse it to ElementTree
        :return: is_valid: is unsupported type
        :return: tree: xml tree
        :return: mybatis_version
        :return: error_msg
        """
        tree = None
        error_msg = ''
        # determine whether the supported xml format
        is_valid = self.check_xml_valid()
        if not is_valid:
            return is_valid, tree, error_msg
        # get file encoding
        read_encoding = get_encoding(self.file_name)
        try:
            with open(self.file_name, encoding=read_encoding) as f:
                try:
                    if read_encoding == 'gbk':
                        # prevent xxe
                        parser = etree.XMLParser(resolve_entities=False,encoding='gbk')
                        tree = etree.parse(f, parser=parser)
                    else:
                        # prevent xxe
                        parser = etree.XMLParser(resolve_entities=False)
                        tree = etree.parse(f, parser=parser)
                except etree.XMLSyntaxError as e:
                    error_msg = f"invalid xml XMLSyntaxError {read_encoding}: {e}"
                    print(error_msg)
        except UnicodeDecodeError:
            error_msg = f"invalid xml UnicodeDecodeError {read_encoding}"
            print(error_msg)
        return is_valid, tree, error_msg

    def parse_xml_content(self, tree: Optional[ElementTree]):
        """
        parse xml tree to sql list
        :param tree: xml tree
        :return: sql_list

        xml format is divided into two types：
            1.with include
                <table sqlname="aff_biz_event">
                <sql id="AffBizEvent.columns">
                id, event_name, description, event_type, unique_id
                </sql>
                <operation name="queryAllEvent" multiplicity="many">
                    <sql>
                        SELECT
                        <include refid="AffBizEvent.columns"/>
                        FROM aff_biz_event
                        WHERE
                            is_deleted = 0 AND status = 1
                            AND l1_domain_code = ?
                    </sql>
                </operation>
            2.crud: "select", "update", "insert", "delete"
                <mapper namespace="mapper.sqlaudit.InfoSchemaMapper">
                <select id="selectColumns" resultType="entity.Column">
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = #{tableName}
                    AND table_schema = #{dbName}
                </select>
        """
        # include may be out of order.
        # need to parse the include content first When traversing the file.
        # direct traversal may result in the content of ref not being obtained.
        include_sql_dict = get_include_define(tree)
        # parse the xml tree and parameterize the assignment and return to sql
        sql_list = parse_mybatis_xml_tree(tree, include_sql_dict)
        return sql_list
