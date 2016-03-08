from __future__ import unicode_literals, print_function
import os
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


def assemble_records(records):
    partitions = {}

    # assumes that auths appear before bibs with auth links in record list
    fetched_auths = set()
    linked_auths = set()

    for record_id in records:
        rectype, recnum = record_id.split('/', 1)

        if not rectype in partitions:
            root = make_root()
            partitions[rectype] = (
                    root,
                    etree.SubElement(root, 'ListRecords'))
        root, reclist = partitions[rectype]

        if rectype == 'auth':
            fetched_auths.add(recnum)

        record = get_record(rectype, recnum)
        if record is None:
            print("found nothing for", record_id, file=stderr)
            continue

        for spec in record.findall('{0}header/{0}setSpec'.format('{%s}' % OAI_NS)):
            if spec.text.startswith('authority:'):
                authnum = spec.text.split(':')[1]
                if authnum not in fetched_auths:
                    linked_auths.add(authnum)

        reclist.append(record)

    for authnum in linked_auths:
        record = get_record('auth', authnum)
        if record is None:
            print("found no auth data for", authnum, file=stderr)
            continue
        partitions['auth'][1].append(record)

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
            print()


OAI_NS = "http://www.openarchives.org/OAI/2.0/"

def make_root():
    root = etree.Element('OAI-PMH', **{'nsmap': {None: OAI_NS}} if lxml else {'xmlns': OAI_NS})
    etree.SubElement(root, 'responseDate').text = "9999-01-01T00:00:00Z"
    etree.SubElement(root, 'request',
            verb="ListRecords",
            resumptionToken="null|2001-12-11T23:00:00Z|107000|null|null|marcxml",
            metadataPrefix="marcxml"
        ).text = "http://data.libris.kb.se/auth/oaipmh"
    return root


FETCH_URL = 'http://data.libris.kb.se/{rectype}/oaipmh/?verb=GetRecord&metadataPrefix=marcxml&identifier=http://libris.kb.se/resource/{rectype}/{recnum}'

def get_record(rectype, recnum):
    url = FETCH_URL.format(**vars())
    res = requests.get(url, auth=(name, passwd), stream=True)
    record_root = etree.parse(res.raw)
    return record_root.find('/*/*')


if __name__ == '__main__':
    args = argv[1:]

    passwd_data = args.pop(0)
    if os.path.isfile(passwd_data):
        import json
        with open(passwd_data) as fp:
            secrets = json.load(fp)
        name, passwd = secrets['oaipmhUsername'], secrets['oaipmhPassword']
    elif ':' in passwd_data:
        name, passwd = passwd_data.split(':')
    else:
        name, passwd = "", ""

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

    assemble_records(records)
