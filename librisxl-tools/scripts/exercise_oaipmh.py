from __future__ import unicode_literals, print_function
import sys
import time
import requests
from lxml import etree


PMH = "{http://www.openarchives.org/OAI/2.0/}"

def parse_oaipmh(start_url, name, passwd, resumption_token=None):
    resumption_token = resumption_token
    while True:
        url = make_next_url(start_url, resumption_token)
        res = requests.get(url, auth=(name, passwd), stream=True, timeout=3600)
        #source = StringIO(res.raw.read())
        root = etree.parse(res.raw)
        resumption_token = root.findtext("{0}ListRecords/{0}resumptionToken".format(PMH))
        yield root, resumption_token
        if not resumption_token:
            break

def make_next_url(base_url, resumption_token=None):
    params = "?verb=ListRecords&resumptionToken=%s" % resumption_token if resumption_token else "?verb=ListRecords&metadataPrefix=marcxml"
    return base_url + params

def process_oaipmh(start_url, name, passwd, resumption_token=None, to_json=True):
    if to_json:
        import json
        from marcutil import xml2json
    start_time = time.time()
    record_count = 0

    for root, next_token in parse_oaipmh(start_url, name, passwd, resumption_token):
        elapsed = time.time() - start_time
        records = root.findall("{0}ListRecords/{0}record".format(PMH))
        record_count += len(records)

        if to_json:
            for record in records:
                marcrec = record.find('{0}metadata/*'.format(PMH))
                if not marcrec:
                    continue
                data = xml2json(marcrec)
                extra = {}
                for elem in record.findall('{0}header/*'.format(PMH)):
                    key = elem.tag.rsplit('}', 1)[-1]
                    extra.setdefault(key, []).append(elem.text)
                if extra:
                    data['_extra'] = extra
                print(json.dumps(data).encode('utf-8'))

        print("Record count: %s. Got resumption token: %s. Elapsed time: %s. "
                "Records/second: %s" % (
                    record_count, next_token, elapsed, record_count / elapsed),
                file=sys.stderr)


if __name__ == '__main__':
    args = sys.argv[1:]

    if not args:
        print("Usage: %s OAI_PMH_URL [NAME:PASSWORD] [RESUMPTION_TOKEN] [-c]" % sys.argv[0])
        exit()

    if '-c' in args:
        args.remove('-c')
        to_json = True
    else:
        to_json = False

    start_url = args.pop(0)
    if args:
        name, passwd = args.pop(0).split(':')
    else:
        name, passwd = None, None

    resumption_token = args.pop(0) if args else None

    process_oaipmh(start_url, name, passwd, resumption_token, to_json)
