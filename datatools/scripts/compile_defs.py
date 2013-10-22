import json
from rdflib import *
from rdflib.namespace import SKOS


BASE = "http://libris.kb.se/"

datasets = {}
def dataset(f):
    datasets[f.__name__] = f


def deref(iri):
    return Graph().parse(iri).resource(iri)

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
            'dc': 'http://purl.org/dc/terms/',
            'code': {'@id': 'notation', '@type': 'dc:ISO639-2'},
            'tag': {'@id': 'notation', '@type': 'dc:ISO639-1'},
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
        langobj = items[code] = {'@id': base + code, '@type': 'Concept', 'iso-639-2': code}
        langobj['matches'] = lang_concept.identifier
        prefLabels = langobj['labelByLang'] = {}
        for label in lang_concept.objects(SKOS.prefLabel):
            prefLabels[label.language] = label
        label_sv = lang_code_map.get(code)
        if label_sv:
            prefLabels['sv'] = label_sv
    return result


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    if not args:
        sys.exit()
    dataset = args.pop(0)
    result = datasets[dataset]()
    print json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False).encode('utf-8')
