import os
import json
from rdflib import *
from rdflib.namespace import SKOS, DCTERMS


datasets = {}
def dataset(f):
    datasets[f.__name__] = f
    return f


BASE = "http://libris.kb.se/"


@dataset
def terms():
    source = Graph().parse('datatools/def/terms.ttl', format='turtle')
    return source.serialize(format="json-ld", auto_compact=True)


@dataset
def schemes():
    source = Graph().parse('datatools/def/schemes.ttl', format='turtle')
    return source.serialize(format="json-ld",
            context="datatools/def/schemes-context.jsonld")


@dataset
def relators():
    pass


@dataset
def languages():
    base = "/def/%s/" % dataset
    with open('whelk-core/src/main/resources/langcodes.json') as f:
        lang_code_map = json.load(f)
    source = Graph().parse('http://id.loc.gov/vocabulary/iso639-2')
    items = {}
    result = {
        '@context': {
            '@base': BASE,
            '@vocab': SKOS,
            'dc': DCTERMS,
            'langCode': {'@id': 'notation', '@type': 'dc:ISO639-2'},
            'langTag': {'@id': 'notation', '@type': 'dc:ISO639-1'},
            'byCode': {'@id': '@graph', '@container': '@index'},
            'matches': {'@id': 'exactMatch', '@type': '@id'},
            'labelByLang': {'@id': 'prefLabel', '@container': '@language'}
        },
        'byCode': items
    }
    for lang_concept in map(source.resource, source.subjects(RDF.type, SKOS.Concept)):
        # TODO: separate codes for ISO 639-2 and ISO 639-1 (described in deref:ed data)
        #deref(lang_concept.identifier)
        code = lang_concept.value(SKOS.notation)
        langobj = items[code] = {'@id': base + code, '@type': 'Concept', 'langCode': code}
        langobj['matches'] = lang_concept.identifier
        prefLabels = langobj['labelByLang'] = {}
        for label in lang_concept.objects(SKOS.prefLabel):
            prefLabels[label.language] = label
        label_sv = lang_code_map.get(code)
        if label_sv:
            prefLabels['sv'] = label_sv
    return result


@dataset
def countries():
    pass


@dataset
def nationalities():
    pass


def deref(iri):
    return Graph().parse(iri).resource(iri)


def serialize(data):
    if isinstance(data, (list, dict)):
        data = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False)
    if isinstance(data, unicode):
        data = data.encode('utf-8')
    return data


if __name__ == '__main__':

    from optparse import OptionParser
    op = OptionParser("Usage: %prog [-h] [-o OUTPUT_DIR] [DATASET..]",
            description="Available datasets: " + ", ".join(datasets))
    op.add_option('-o', '--output', type=str, help="Output directory")
    opts, args = op.parse_args()

    if not args:
        if opts.output:
            args = list(datasets)
        else:
            op.print_usage()
            op.exit()

    import sys

    for dataset in args:
        if len(args) > 1:
            print "Dataset:", dataset
        result = datasets[dataset]()
        data = serialize(result)
        if data and opts.output:
            outdir = os.path.join(opts.output, dataset)
            if not os.path.isdir(outdir):
                os.makedirs(outdir)
            outfile = os.path.join(outdir, "data.jsonld")
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
