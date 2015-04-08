#s!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, json, argparse, base64
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

args = None
es = None
expansiondata = {}

def loadexpansiondata(identifier, dataset, source):
    if identifier in expansiondata:
        return expansiondata.get(identifier)

    print("No cached expansiondata found for " + identifier + ". Startin query ...")
    query = {
        "query": {
            "term" : { "about.@id" : identifier }
        }
    }
    result  = es.search(index=args['index'],
                       doc_type=dataset,
                       _source=source,
                       size=1,
                       body=query)
    data = None
    if 'hits' in result:
        try:
            data = result.get("hits").get("hits")[0].get("_source").get("about")
        except:
            print("No hits for {0}".format(identifier))
    expansiondata[identifier] = data
    return data


def linkexpand(about):
    try:
        for (key,value) in about.items():
            if key in ['attributedTo','influencedBy','author','language','originalLanguage','literaryForm','publicationCountry']:
                items = value if type(value) is list else [ value ]
                expandeditems = []
                for item in items:
                    identifier = item.get('@id')
                    expansion = None
                    if identifier and identifier.startswith('/resource/'):
                        expansion = loadexpansiondata(identifier, 'auth', [ "about.@id", "about.@type", "about.birthYear", "about.deathYear", "about.familyName", "about.givenName" ])
                    if identifier and identifier.startswith('/def/'):
                        expansion = loadexpansiondata(identifier, 'def', [ "about.*" ])

                    expandeditems.append(expansion if expansion else item)

                about[key] = expandeditems if len(expandeditems) > 1 else expandeditems[0]

    except Exception as e:
        print("Problem link expanding doc", e)

    return about


def filter(jsondata, dataset):
    if not 'about' in jsondata or dataset in ['auth','hold','sys','def']:
        return jsondata

    about = jsondata['about']

    about = linkexpand(about)

    try:
        if 'attributedTo' in about:
            about['creator'] = about['attributedTo']
        if 'identifier' in about:
            about['isbn'] = [ ident['identifierValue'].replace("-","") 
                             for ident in about['identifier'] 
                             if ident.get('identifierScheme', {'@id':False}).get('@id') == "/def/identifiers/isbn" and 'identifierValue' in ident]
        about['title'] = about['instanceTitle']['titleValue']
    except Exception as e:
        print("Problem with term reducing", e)

    jsondata['about'] = about

    return jsondata

def reindex(**args):
    con = psycopg2.connect(database=args['database'], user=args['user'], host=args['host'])
    cur = con.cursor()

    if args['dataset']:
        query = "SELECT identifier,dataset,data FROM libris WHERE dataset = '%s' AND  NOT entry @> '{ \"deleted\" : true }'" % args['dataset']
    else:
        query = "SELECT identifier,dataset,data FROM libris WHERE NOT entry @> '{ \"deleted\" : true }'"

    cur.execute(query)
    print("Query executed, start reading rows.")

    counter = 0
    jsoncounter = 0
    while True:
        results = cur.fetchmany(2000)
        docs = []

        if not results:
            break

        for row in results:
            counter += 1
            try:
                identifier = base64.urlsafe_b64encode(bytes(row[0], 'utf-8')).decode()
                dataset = row[1]
                stored_json = json.loads(bytes(row[2]).decode("utf-8"))
                index_json = filter(stored_json, dataset)
                docs.append({ '_index': args['index'], '_type': dataset, '_id' : identifier, '_source': index_json })
                jsoncounter += 1
            except Exception as e:
                print("Failed to convert row {0} to json".format(row[0]), e)
                raise

        r = bulk(es, docs)
        es.cluster.health(wait_for_status='yellow', request_timeout=10)

    print("save result", r)
    print("All {0} rows read. {1} where json data.".format(counter, jsoncounter))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='LIBRISXL reindexer')
    parser.add_argument('--elasticsearch', help='Elasticsearch connect url', default='localhost:9200', nargs='+')
    parser.add_argument('--dataset', help='Which dataset to reindex')
    parser.add_argument('--database', help='The name of the postgresql database schema. Defaults to "whelk"', default='whelk')
    parser.add_argument('--user', help='Username for the postgresql database. Defaults to "whelk"', default='whelk')
    parser.add_argument('--host', help='The postgresql host to connect to. Defaults to "localhost"', default='localhost')
    parser.add_argument('--password', help='Password for the postgresql database.')
    parser.add_argument('--index', help='Which index to save to', required=True)

    args = vars(parser.parse_args())
    es = Elasticsearch(args['elasticsearch'], sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=10, timeout=30)

    reindex(**args)

