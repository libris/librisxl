import sys
import json
from lxml import etree
from marcutil import xml2json

if len(sys.argv) > 1:
    fpath = sys.argv[1]
    with open(fpath) as f:
        root = etree.parse(f)
else:
    root = etree.parse(sys.stdin)

data = xml2json(root)
json.dump(data, sys.stdout)
