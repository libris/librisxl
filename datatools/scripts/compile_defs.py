import os
import sys
import json
from rdflib import *
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
            },
            index=('index', '#'))


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
            "@base": BASE, "@language": "sv"
            }, index=('byNotation', 'notation'))

    index = data['byNotation']

    # TODO: camelCase label_en for most terms, plus separate relatorcodes-overlay.json
    with open(scriptpath('../source/relatorcodes.json')) as fp:
        for extra in json.load(fp):
            code = extra.pop('code')
            node = index.get(code)
            iri = "/def/relators/%s" % extra.pop('term')
            if not node:
                index[code] = extra
                extra['@id'] = iri
            else:
                extra['@id'] = iri
                extra['equivalentProperty'] = node['@id']
                node.update(extra)

    return data


@dataset
def languages():
    with open(scriptpath('../source/langcodes.json')) as fp:
        lang_code_map = json.load(fp)

    source = cached_rdf('http://id.loc.gov/vocabulary/iso639-2')

    result = {
        '@context': [
            "/def/context/skos.jsonld",
            {
                "@base": BASE,
                "langCode": {"@id": "notation", "@type": "dc:ISO639-2"},
                "langTag": {"@id": "notation", "@type": "dc:ISO639-1"},
                "byCode": {"@id": "@graph", "@container": "@index"}
            }
        ]
    }
    result['byCode'] = items = {}

    ISO639_1Lang = URIRef("http://id.loc.gov/vocabulary/iso639-1/iso639-1_Language")

    for lang_concept in map(source.resource, source.subjects(RDF.type, SKOS.Concept)):
        code = unicode(lang_concept.value(SKOS.notation))
        iri = "/def/languages/" + code
        langobj = items[code] = {'@id': iri, '@type': 'Concept', 'langCode': code}
        langobj['matches'] = lang_concept.identifier

        langdef = deref(lang_concept.identifier)
        for variant in langdef[SKOS.exactMatch]:
            if variant.graph[variant.identifier : RDF.type : ISO639_1Lang]:
                iso639_1 = variant.value(SKOS.notation)
                langobj['tag'] = iso639_1

        for label in lang_concept[SKOS.prefLabel]:
            if label.language == 'en':
                langobj['prefLabel_en'] = unicode(label)
                break

        label_sv = lang_code_map.get((code))
        if label_sv:
            langobj['prefLabel'] = label_sv

    return result


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

    index = data['byNotation']
    with open(scriptpath('../source/countrycodes.json')) as fp:
        labelmap = json.load(fp)
    for key, label in labelmap.items():
        node = index.get(key)
        if node and 'label' not in node:
            node['label'] = label

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


def deref(iri):
    return cached_rdf(iri).resource(iri)


CACHEDIR = None

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


def _serialize(data):
    if isinstance(data, (list, dict)):
        data = json.dumps(data, indent=2, sort_keys=True,
                separators=(',', ': '), ensure_ascii=False)
    if isinstance(data, unicode):
        data = data.encode('utf-8')
    return data


def run(names, output, cache):
    global CACHEDIR
    if cache:
        CACHEDIR = cache
    for name in names:
        if len(names) > 1:
            print "Dataset:", name
        result = datasets[name]()
        data = _serialize(result)
        if data and output:
            outfile = os.path.join(output, "%s.jsonld" % name)
            outdir = os.path.dirname(outfile)
            if not os.path.isdir(outdir):
                os.makedirs(outdir)
            fp = open(outfile, 'w')
        else:
            fp = sys.stdout
        try:
            if data:
                fp.write(data)
            else:
                print "N/A"
            print
        finally:
            if fp is not sys.stdout:
                fp.close()


if __name__ == '__main__':

    from optparse import OptionParser
    op = OptionParser("Usage: %prog [-h] [-o OUTPUT_DIR] [DATASET..]",
            description="Available datasets: " + ", ".join(datasets))
    op.add_option('-o', '--output', type=str, help="Output directory")
    op.add_option('-c', '--cache', type=str, help="Cache directory")
    opts, args = op.parse_args()

    if not args:
        if opts.output:
            args = list(datasets)
        else:
            op.print_usage()
            op.exit()

    run(args, opts.output, opts.cache)
