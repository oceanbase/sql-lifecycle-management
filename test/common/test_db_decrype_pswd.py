#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

from common.db_decrypt_pswd import decrypt_password


class TestEnDeTxt(unittest.TestCase):
    def test_de(self):
        text = b'c7b239afa64fe3fe0bf3cb262721f0df'
        de_text = decrypt_password(text)
        print('decrypt', text, de_text)
        self.assertTrue(de_text == 'tarscore')


if __name__ == '__main__':
    unittest.main()
