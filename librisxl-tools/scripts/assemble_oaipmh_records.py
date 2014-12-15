import csv
import requests
try:
    import lxml
    from lxml import etree
except ImportError:
    raise
    #lxml = None
    #from xml.etree import ElementTree as etree
from sys import argv, stdout, stderr
import os

baseurl = 'http://data.libris.kb.se/{rectype}/oaipmh/?verb=GetRecord&metadataPrefix=marcxml&identifier=http://libris.kb.se/resource/{record}'

args = argv[1:]
name, passwd = args.pop(0).split(':')

if args[0].endswith('.tsv'):
    records = []
    with open(args[0], 'rb') as fp:
        reader = csv.reader(fp, dialect='excel-tab')
        for row in reader:
            if row:
                records.append(row[0])
    if args[1]:
        outdir = args[1]
else:
    outdir = None
    records = args

OAI_NS = "http://www.openarchives.org/OAI/2.0/"

def make_root():
    root = etree.Element('OAI-PMH', **{'nsmap': {None: OAI_NS}} if lxml else {'xmlns': OAI_NS})
    etree.SubElement(root, 'responseDate').text = "1970-01-01T00:00:00Z"
    etree.SubElement(root, 'request',
            verb="ListRecords",
            resumptionToken="null|2001-12-11T23:00:00Z|107000|null|null|marcxml",
            metadataPrefix="marcxml"
        ).text = "http://data.libris.kb.se/auth/oaipmh"
    return root

partitions = {}
for record in records:
    rectype, recid = record.split('/')
    if not rectype in partitions:
        root = make_root()
        partitions[rectype] = (
                root,
                etree.SubElement(root, 'ListRecords'))
    root, reclist = partitions[rectype]
    url = baseurl.format(**vars())
    res = requests.get(url, auth=(name, passwd), stream=True)
    record_root = etree.parse(res.raw)
    record_data = record_root.find('/*/*')
    if record_data is None:
        print >>stderr, "found nothing for", record
        continue
    reclist.append(record_data)

for name, (root, reclist) in partitions.items():
    if outdir:
        fpath = os.path.join(outdir, name, "oaipmh")
        fdir = os.path.dirname(fpath)
        if not os.path.isdir(fdir):
            os.makedirs(fdir)
        fp = open(fpath, 'w')
    else:
        fp = stdout
    kwargs = {'pretty_print': True} if lxml else {}
    fp.write(etree.tostring(root, encoding='UTF-8', **kwargs))
    if outdir:
        fp.close()
    else:
        print
