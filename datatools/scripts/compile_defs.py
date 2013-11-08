import os
import sys
import re
import json
import csv
from rdflib import Graph, URIRef, RDF, RDFS, OWL
from rdflib.namespace import SKOS, DCTERMS
from rdflib_jsonld.serializer import from_rdf


scriptpath = lambda pth: os.path.join(os.path.dirname(__file__), pth)


BASE = "http://libris.kb.se/"


datasets = {}
def dataset(func):
    datasets[func.__name__] = func
    return func


@dataset
def terms():
    source = Graph().parse(scriptpath('../def/terms.ttl'), format='turtle')
    return to_jsonld(source, "owl", {
            "index": {"@id": "@graph", "@container": "@index"},
            "@language": "sv"
            }, index=('index', '#'))


@dataset
def schemes():
    source = Graph().parse(scriptpath('../def/schemes.ttl'), format='turtle')
    return to_jsonld(source, "skos", {
            "byNotation": {"@id": "@graph", "@container": "@index"},
            "@base": BASE,
            "@language": "sv"
            }, index=('byNotation', 'notation'))


@dataset
def relators():
    source = cached_rdf('http://id.loc.gov/vocabulary/relators')
    source = filter_graph(source, (RDF, RDFS, OWL, SKOS, DCTERMS),
            oftype=OWL.ObjectProperty)

    data = to_jsonld(source, "owl", {
            "byNotation": {"@id": "@graph", "@container": "@index"},
            "@base": BASE,
            "@language": "sv"
            }, index=('byNotation', 'notation'))

    extend(data['byNotation'], 'funktionskoder.tsv', 'sv',
            term_source='label_en', iri_template="/def/relators/{term}",
            addtype='ObjectProperty',
            relation='equivalentProperty')

    return data


@dataset
def languages(partition=True):
    source = cached_rdf('http://id.loc.gov/vocabulary/iso639-2')

    basecontext = "/def/context/skos.jsonld"
    langcontext = {
        "@base": BASE,
        "@language": "sv"
    }
    dataset = {
        '@context': [
            basecontext,
            dict(langcontext, byCode={"@id": "@graph", "@container": "@index"})
        ]
    }

    dataset['byCode'] = items = {}
    subdocs = {}

    extras = load_data(scriptpath('../source/spraakkoder.tsv'))

    ISO639_1Lang = URIRef("http://id.loc.gov/vocabulary/iso639-1/iso639-1_Language")

    for lang_concept in map(source.resource, source.subjects(RDF.type, SKOS.Concept)):
        code = unicode(lang_concept.value(SKOS.notation))

        summary = items[code] = {
            '@id': "/def/languages/%s" % code,
            '@type': 'Concept',
            'langCode': code
        }
        if partition:
            subdoc = subdocs[code] = {"@context": [basecontext, langcontext]}
            subdoc.update(summary)
        else:
            subdoc = summary
        subdoc['matches'] = lang_concept.identifier

        langdef = deref(lang_concept.identifier)
        for variant in langdef[SKOS.exactMatch]:
            if variant.graph[variant.identifier : RDF.type : ISO639_1Lang]:
                iso639_1 = variant.value(SKOS.notation)
                subdoc['langTag'] = iso639_1

        for label in lang_concept[SKOS.prefLabel]:
            if label.language == 'en':
                subdoc['prefLabel_en'] = unicode(label)
                break

        item = extras.get((code))
        if item:
            summary['prefLabel'] = subdoc['prefLabel'] = item['label_sv']

    if partition:
        return dataset, subdocs
    else:
        return dataset


@dataset
def countries():
    source = cached_rdf('http://id.loc.gov/vocabulary/countries')
    source = filter_graph(source, (RDF, RDFS, OWL, SKOS, DCTERMS),
            oftype=SKOS.Concept)

    data = to_jsonld(source, "skos", {
            "byNotation": {"@id": "@graph", "@container": "@index"},
            "@base": BASE,
            "@language": "sv"
            }, index=('byNotation', 'notation'))

    extend(data['byNotation'], 'landskoder.tsv', 'sv',
            iri_template="/def/countries/{notation}",
            relation='exactMatch')

    return data


@dataset
def nationalities():
    pass


def filter_graph(source, propspaces, oftype=None):
    propspaces = tuple(map(unicode, propspaces))
    okspace = lambda t: any(t.startswith(ns) for ns in propspaces)
    selected = set(source[:RDF.type:oftype]) if oftype else None

    graph = Graph()
    for s, p, o in source:
        if selected and s not in selected:
            continue
        if not okspace(p) or p == RDF.type and oftype and o != oftype:
            continue
        graph.add((s, p, o))

    return graph


def to_jsonld(source, contextref, contextobj=None, index=None):
    contextpath = scriptpath("../def/context/%s.jsonld" % contextref)
    contexturi = "/def/context/%s.jsonld" % contextref
    context = [contextpath, contextobj] if contextobj else contextpath
    data = from_rdf(source, context_data=context)
    data['@context'] = [contexturi, contextobj] if contextobj else contexturi

    # customize to a convenient shape (within the bounds of JSON-LD)
    if index:
        graph_key, index_key = index
        nodes = data.pop(graph_key)
        graphmap = data[graph_key] = {}
    else:
        nodes = data['@graph']
        index_key = None
    base = contextobj.get('@base')
    for node in nodes:
        nodeid = node['@id']
        if base and nodeid.startswith(base):
            node['@id'] = nodeid[len(base)-1:]
        elif nodeid.startswith('_:'):
            del node['@id'] # TODO: lossy if referenced, should be embedded..
        if index_key:
            key = None
            if index_key in ('#', '/'):
                leaf = node['@id'].rsplit(index_key, 1)[-1]
                key = leaf or node['@id']
            elif index_key in node:
                key = node[index_key]
            if key:
                graphmap[key] = node

    return data


def extend(index, extradata, lang, keys=('label', 'comment'),
        term_source=None, iri_template=None, addtype=None, relation=None):
    fpath = scriptpath('../source/%s' % extradata)
    extras = load_data(fpath)
    for key, item in extras.items():
        iri = None
        if iri_template:
            term = item.pop('term', None)
            if not term and term_source:
                term = to_camel_case(item[term_source])
            if term:
                iri = iri_template.format(term=term)
        node = index.get(key)
        if not node:
            if iri:
                node = index[key] = {}
                if addtype:
                    node['@type'] = addtype
            else:
                continue
        for key in keys:
            item_key = "%s_%s" % (key, lang) if lang else key
            if item_key in item:
                node[key] = item[item_key]
        if not iri and iri_template:
            iri = iri_template.format(**node)
        if iri:
            node['@id'], orig_iri = iri, node.get('@id')
            if relation and orig_iri:
                node[relation] = orig_iri

def to_camel_case(label):
    return "".join((s[0].upper() if i else s[0].lower()) + s[1:]
            for (i, s) in enumerate(re.split(r'[\s,.-]', label)) if s)


def load_data(fpath):
    csv_dialect = ('excel' if fpath.endswith('.csv')
            else 'excel-tab' if fpath.endswith('.tsv')
            else None)
    if csv_dialect:
        with open(fpath, 'rb') as fp:
            reader = csv.DictReader(fp, dialect=csv_dialect)
            return {item.pop('code'):
                        {k: v.decode('utf-8').strip()
                            for (k, v) in item.items() if v}
                    for item in reader}
    else:
        with open(fpath) as fp:
            return json.load(fp)


CACHEDIR = None

def deref(iri):
    return cached_rdf(iri).resource(iri)

def cached_rdf(fpath):
    source = Graph()
    http = 'http://'
    if not CACHEDIR:
        print >> sys.stderr, "No cache directory configured"
    elif fpath.startswith(http):
        remotepath = fpath
        fpath = os.path.join(CACHEDIR, remotepath[len(http):]) + '.ttl'
        if not os.path.isfile(fpath):
            fdir = os.path.dirname(fpath)
            if not os.path.isdir(fdir):
                os.makedirs(fdir)
            source.parse(remotepath)
            source.serialize(fpath, format='turtle')
            return source
        else:
            return source.parse(fpath, format='turtle')
    return source.parse(fpath)


def run(names, outdir, cache):
    global CACHEDIR
    if cache:
        CACHEDIR = cache
    for name in names:
        if len(names) > 1:
            print "Dataset:", name
        resultset = {}
        data = datasets[name]()
        if isinstance(data, tuple):
            data, subdocs = data
            resultset = {}
            resultset["%s/index" % name] = data
            for subname, subdoc in subdocs.items():
                resultset["%s/%s" % (name, subname)] = subdoc
        else:
            resultset[name] = data
        for name, data in resultset.items():
            _output(name, data, outdir)
        print

def _output(name, data, outdir):
    result = _serialize(data)
    if result and outdir:
        outfile = os.path.join(outdir, "%s.jsonld" % name)
        outdir = os.path.dirname(outfile)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        fp = open(outfile, 'w')
    else:
        fp = sys.stdout
    try:
        if result:
            fp.write(result)
        else:
            print "N/A"
    finally:
        if fp is not sys.stdout:
            fp.close()

def _serialize(data):
    if isinstance(data, (list, dict)):
        data = json.dumps(data, indent=2, sort_keys=True,
                separators=(',', ': '), ensure_ascii=False)
    if isinstance(data, unicode):
        data = data.encode('utf-8')
    return data


if __name__ == '__main__':

    from optparse import OptionParser
    op = OptionParser("Usage: %prog [-h] [-o OUTPUT_DIR] [DATASET..]",
            description="Available datasets: " + ", ".join(datasets))
    op.add_option('-o', '--outdir', type=str, help="Output directory")
    op.add_option('-c', '--cache', type=str, help="Cache directory")
    opts, args = op.parse_args()

    if not args:
        if opts.outdir:
            args = list(datasets)
        else:
            op.print_usage()
            op.exit()

    run(args, opts.outdir, opts.cache)
