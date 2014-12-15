#!/usr/bin/env python

import os
import json
from sys import argv

basepath = argv[1]

for root, dirs, files in os.walk(basepath):
    if 'entry.json' in files:
        json_data = open('%s/entry.json' % root, 'r')
        entry = json.load(json_data)
        json_data.close()
        specs = entry['meta'].get('oaipmhSetSpec', [])
        for spec in specs:
            spec = spec.replace('auth:', 'authority:')
            spec = spec.replace('bib:', 'bibid:')

        for link in entry['meta'].pop('link', []):
            (foo, dataset, number) = link.split("/")
            if dataset == "auth":
                dataset = "authority"
            if dataset == "bib":
                dataset = "bibid"
            set_spec = ":".join([dataset, number])
            print "fixing %s/entry.json (%s -> %s)" % (root, link, set_spec)
            specs.append(set_spec)

        if specs:
            entry['meta']['oaipmhSetSpec'] = specs
            json_data = open('%s/entry.json' % root, 'w')
            json.dump(entry, json_data)
            json_data.close()
