# -*- coding: utf-8 -*-

import hashlib


def md5(md):
    s = md.encode('utf-8')
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()
