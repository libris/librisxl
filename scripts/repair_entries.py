#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
from sys import argv

basepath = argv[1]

for root, dirs, files in os.walk(basepath):
    if 'entry.json' in files:
        filename = next(f for f in files if f.endswith('.jsonld'))
        print "fixing %s/entry.json (%s)" % (root, filename)
        json_data = open('%s/entry.json' % root, 'r')
        entry = json.load(json_data)
        json_data.close()
        entry['entry']['dataFileName'] = filename
        json_data = open('%s/entry.json' % root, 'w')
        json.dump(entry, json_data)
        json_data.close()



