#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import unittest
from common.db_decrypt_pswd import encrypt_password, decrypt_password
from common.db_pool import ConnDBOperate
from common.db_query import metadb


class TestEnDeTxt(unittest.TestCase):
    def test_de(self):
        text = b'c7b239afa64fe3fe0bf3cb262721f0df'
        de_text = decrypt_password(text)
        print('decrypt',text,de_text)
        self.assertTrue(de_text == 'tarscore')

    def test_cfg(self):
        meta_info = ConnDBOperate(metadb)
        get_sql = '''SELECT * from database_asset limit 1'''
        get_rst = meta_info.func_select_storedb(get_sql)
        meta_info.disconn_storedb()


if __name__ == '__main__':
    unittest.main()
