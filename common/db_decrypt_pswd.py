# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import binascii

from Crypto.Cipher import Blowfish


class ObPassword(object):
    key = b'gQzLk5tTcGYlQ47GG29xQxfbHIURCheJ'
    blowfish = Blowfish.new(key, Blowfish.MODE_ECB)

    @classmethod
    def encode_password(cls, password):
        len_packing = 8 - len(password) % 8
        appendage = chr(len_packing) * len_packing
        return binascii.hexlify(cls.blowfish.encrypt(password + appendage))

    @classmethod
    def decode_password(cls, en_password):
        packed_password = cls.blowfish.decrypt(binascii.unhexlify(en_password))
        len_packing = ord(chr(packed_password[-1]))
        return packed_password[0:(-1 * len_packing)]


def decrypt_password(password_encode):
    de_password = ObPassword.decode_password(password_encode)
    if isinstance(de_password, bytes):
        de_password = str(de_password.decode())
    return de_password


def encrypt_password(password_decode):
    en_password = ObPassword.encode_password(password_decode)
    if isinstance(en_password, bytes):
        en_password = str(en_password.decode())
    return en_password
