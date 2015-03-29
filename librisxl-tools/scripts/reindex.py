#s!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, json, argparse, base64
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def loadexpansiondata(identifier, es, dataset, source):
    query = {
        "query": {
            "term" : { "about.@id" : identifier }
        }
    }
    result  = es.search(index='libris_index',
                       doc_type=dataset,
                       _source=source,
                       size=1,
                       body=query)
    if 'hits' in result:
        try:
            return result.get("hits").get("hits")[0].get("_source").get("about")
        except:
            print("No hits for {0}".format(identifier))
    return None


def linkexpand(about, es):
    try:
        for (key,value) in about.items():
            if key in ['attributedTo','influencedBy','author','language','originalLanguage','literaryForm','publicationCountry']:
                items = value if type(value) is list else [ value ]
                expandeditems = []
                for item in items:
                    identifier = item.get('@id')
                    expansion = None
                    if identifier and identifier.startswith('/resource/'):
                        expansion = loadexpansiondata(identifier, es, 'auth', [ "about.@id", "about.@type", "about.birthYear", "about.deathYear", "about.familyName", "about.givenName" ])
                    if identifier and identifier.startswith('/def/'):
                        expansion = loadexpansiondata(identifier, es, 'def', [ "about.*" ])

                    expandeditems.append(expansion if expansion else item)

                about[key] = expandeditems if len(expandeditems) > 1 else expandeditems[0]

    except Exception as e:
        print("Problem link expanding doc", e)
        raise

    return about


def filter(jsondata, dataset, es):
    # First filter: term reduce
    about = jsondata['about']
    if not about or dataset == 'def' or dataset == 'auth':
        return jsondata

    about = linkexpand(about, es)

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
        raise

    jsondata['about'] = about

    return jsondata

def reindex(**args):
    es = Elasticsearch(args['es'], sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
    con = psycopg2.connect(database='whelk')
    cur = con.cursor()

    if args['ds']:
        query = "SELECT identifier,dataset,data FROM libris WHERE dataset = '{0}'".format(args['ds'])
    else:
        query = "SELECT identifier,dataset,data FROM libris"

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
                identifier = row[0]
                dataset = row[1]
                stored_json = json.loads(bytes(row[2]).decode("utf-8"))
                index_json = stored_json # filter(stored_json, dataset, es)
                docs.append({ '_index': args['index'], '_type': dataset, '_id' : bytes.decode(base64.urlsafe_b64encode(bytes(identifier, 'UTF-8'))) , '_source': index_json })
                jsoncounter += 1
            except Exception as e:
                print("Failed to convert row {0} to json".format(identifier), e)

        r = bulk(es, docs)

    print("save result", r)
    print("All {0} rows read. {1} where json data.".format(counter, jsoncounter))




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='LIBRISXL reindexer')
    parser.add_argument('--es', help='Elasticsearch connect url', default='localhost:9200', nargs='+')
    parser.add_argument('--pg', help='Postgres connection url', default='pg://localhost/whelk')
    parser.add_argument('--ds', help='Which dataset to reindex')
    parser.add_argument('--index', help='Which index to save to', required=True)

    try:
        args = vars(parser.parse_args())
    except:
        exit(1)

    reindex(**args)

