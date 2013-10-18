import json
from lxml import etree

from sys import argv, stdout
fpath = argv[1]
with open(fpath) as f:
    data = json.load(f)

root = etree.Element('record', nsmap={None: 'http://www.loc.gov/MARC21/slim'})
root.set('type', "Bibliographic")
etree.SubElement(root, 'leader').text = data['leader']

for field in data['fields']:
    for k, o in field.items():
        if isinstance(o, unicode):
            e = etree.SubElement(root, 'controlfield')
            e.set('tag', k)
            e.text = o
        else:
            e = etree.SubElement(root, 'datafield')
            e.set('tag', k)
            for ind in ["ind1", "ind2"]:
                e.set(ind, o.get(ind))
            for sub in o['subfields']:
                for code, v in sub.items():
                    subelem = etree.SubElement(e, 'subfield')
                    subelem.set('code', code)
                    subelem.text = v

stdout.write(etree.tostring(root, pretty_print=True, encoding='UTF-8'))
