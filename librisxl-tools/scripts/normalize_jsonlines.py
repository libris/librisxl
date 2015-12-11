#!/usr/bin/env python
from __future__ import unicode_literals, print_function
import unicodedata
import json
import sys
for l in sys.stdin:
    l = json.loads(l)
    l = json.dumps(l, ensure_ascii=False)
    l = unicodedata.normalize('NFC', l)
    print(l.encode('utf-8'))
