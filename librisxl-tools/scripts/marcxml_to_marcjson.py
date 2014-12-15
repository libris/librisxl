import json
from lxml import etree
import sys

if len(sys.argv) > 1:
    fpath = sys.argv[1]
    with open(fpath) as f:
        root = etree.parse(f)
else:
    root = etree.parse(sys.stdin)

MARC = '{http://www.loc.gov/MARC21/slim}'

data = {}
fields = []

for elem in root.findall('*'):
    if elem.tag == MARC+'leader':
        data['leader'] = elem.text
    elif elem.tag == MARC+'controlfield':
        fields.append({elem.get('tag'): elem.text})
    else:
        fields.append({elem.get('tag'): {
                'ind1': elem.get('ind1'),
                'ind2': elem.get('ind2'),
                'subfields': [{sub.get('code'): sub.text}
                              for sub in elem.findall('*')]}})

data['fields'] = fields

json.dump(data, sys.stdout)
