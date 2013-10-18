from collections import OrderedDict
import json
from rdflib import BNode, Graph, Literal, Namespace, RDF, RDFS, OWL
from rdflib.namespace import SKOS
from rdflib.collection import Collection


def parse_marcmap(f):
    return json.load(f, object_pairs_hook=OrderedDict)


entity_map = {
    'Books': 'Book',
    'Maps': 'Map',
    'Serials': 'Serial',
    'Computer': 'DigitalResource',
    'ComputerFile': 'DigitalResource',
    'ConceptOrObjectOrEventOrPlace': 'Resource',
    'CorporateBody': 'Organization',
    'Expression': 'Work', # Instance?
    'Manifestation': 'Instance',
    'ManifestationOrItem': 'Instance',
    'Unspecified': None,
}

# like indicators, may affect other properties (e.g. datatype, source)
ambiguous_entities = ('DataElement', 'Field', 'Segment')

# TODO: incomplete
tag_rel_entity_map = {
    '100': (None, 'Person'), # creator or via relatorCode
    '110': (None, 'Organization'),
    '111': (None, 'Meeting'),
    '130': ('primaryTopic', 'Concept'),
    '400': (None, 'Person'), # skip
    '5xx': ('hasAnnotation', 'Annotation'),
    '600': ('subject', 'Person'),
    '610': ('subject', 'Organization'),
    '611': ('subject', 'Meeting'),
    '630': ('subject', 'Concept'),
    '650': ('subject', 'Concept'),
    '651': ('subject', 'Place'),
    '655': ('subject', 'Concept'),
    '700': ('contributor', 'Person'),
}

fixprop_typerefs = {
    '000': ['typeOfRecord', 'bibLevel'],
    '007': [
        'mapMaterial',
        'computerMaterial',
        'globeMaterial',
        'tacMaterial',
        'projGraphMaterial',
        'microformMaterial',
        'nonProjMaterial',
        'motionPicMaterial',
        'kitMaterial',
        'notatedMusicMaterial',
        'remoteSensImageMaterial', #'soundKindOfMaterial',
        'soundMaterial',
        'textMaterial',
        'videoMaterial'
    ],
    '008': [
        'booksContents', #'booksItem', 'booksLiteraryForm', 'booksBiography',
        'computerTypeOfFile',
            #'mapsItem', #'mapsMaterial', # TODO: c.f. 007.mapMaterial
        #'mixedItem',
        #'musicItem', #'musicMatter', 'musicTransposition',
        'serialsTypeOfSerial', #'serialsItem',
        'visualMaterial', #'visualItem',
    ]
}

def get_proper_entity(name):
    return entity_map.get(name, name)

def get_tag_entity(tag, tagdef):
    tag_entity, rel = get_proper_entity(tagdef.get('entity')), None
    mapped = None
    if tag_entity in ('DataElement', 'Field'):
        mapped = tag_rel_entity_map.get(tag[0]+'xx')
        #print "# TODO: map", tag, tag_entity
        tag_entity = None
    if not mapped:
        mapped = tag_rel_entity_map.get(tag)
    if mapped:
        rel, tag_entity = mapped
    return tag_entity

def upfirst(s):
    """
    >>> upfirst('someTerm')
    'SomeTerm'
    """
    return s[0].upper() + s[1:]


SCHEMA = Namespace("http://schema.org/")
VANN = Namespace("http://purl.org/vocab/vann/")
#BF = Namespace("http://bibframe.org/vocab/")
LF = Namespace("http://purl.org/net/927/libframe#")
LTAX = Namespace("http://purl.org/net/927/libframe/taxonomy#")

DOMAIN = SCHEMA.domain


graph = Graph()
for key, obj in vars().items():
    if isinstance(obj, Namespace):
        graph.namespace_manager.bind(key.lower(), obj)
graph.namespace_manager.bind(None, LF)

graph.resource(LF.Work).add(RDF.type, OWL.Class)
graph.resource(LF.Instance).add(RDF.type, OWL.Class)


def vocab_from_marcmap(marcmap):
    bib = marcmap['bib']
    process_leader(bib)
    process_fix_media_types(bib, '008')
    process_fix_media_types(bib, '006')
    process_fix_media_types(bib, '007')
    _fixed_cleanup()
    for tag, tagdef in bib.items():
        if tag in ('000', '006', '007', '008', 'fixprops'):
            continue
        process_tag(tag, tagdef)
    _cleanup()

def process_leader(bib):
    leader = bib['000']
    fixprops = bib['fixprops']
    for col in leader['fixmaps'][0]['columns']:
        entity = col.get('entity')
        if not entity: continue
        entity = get_proper_entity(entity)
        eclass = graph.resource(LF[entity])
        eclass.add(RDF.type, OWL.Class)
        _process_fixprop('000', None, col, fixprops)
        _process_property(col, [eclass], '000', col)

def process_fix_media_types(bib, tag):
    tagdef = bib[tag]
    fixprops = bib['fixprops']
    baseclass = LF.Instance if tag == '007' else LF.Work

    for fixmap in tagdef['fixmaps']:
        term = fixmap.get('term') or fixmap.get('name').split('_', 1)[1]
        term = get_proper_entity(term)
        if term:
            klass = graph.resource(LF[term])
            klass.add(RDF.type, OWL.Class)
            klass.add(RDFS.subClassOf, baseclass)
            # TODO: generate combinations of mediaType and bibLevel?
            #for recTypeBibLevel in fixmap.get('matchRecTypeBibLevel', []):
            #    bibLevel = fixprops['bibLevel'].get(recTypeBibLevel[1])
            #    if bibLevel and 'id' in bibLevel:
            #        bl_term = upfirst(bibLevel['id'])
            #        combined = graph.resource(LF[term + bl_term])
            #        combined.add(RDFS.subClassOf, LF[bl_term])
            #        combined.add(RDFS.subClassOf, klass)
        else:
            klass = baseclass

        for col in fixmap['columns']:
            _process_fixprop(tag, klass, col, fixprops)
            domains = [klass]
            entity = col.get('entity')
            entity = get_proper_entity(entity)
            if entity and entity != 'Work':
                eclass = graph.resource(LF[entity])
                eclass.add(RDF.type, OWL.Class)
                if entity == 'Item':
                    domains.append(eclass)
                elif entity != 'Instance':
                    domains = [eclass]
            _process_property(col, domains, tag, col)

def process_tag(tag, tagdef):
    tag_entity = get_tag_entity(tag, tagdef)

    tag_concept = graph.resource(LTAX[tagdef.get('id') or "field-" + tag])
    tag_concept.add(RDF.type, SKOS.Concept)
    tag_concept.add(SKOS.notation, Literal(tag))
    _add_labels(tag_concept, tagdef)

    # TODO: if rel, add it too?
    #if ind(1|2)_entity == 'DataElement': specializes col (a?) (e.g. format, specific prop)
    #if ind(1|2)_entity == 'Field': sometimes another prop (sometimes boolean)?

    cols = tagdef['subfield'].items() if 'subfield' in tagdef else [(None, tagdef)]
    for code, col in cols:
        entity = col.get('entity')
        entity = get_proper_entity(entity)
        if entity in ambiguous_entities:
            continue
        domainclass = None
        if entity:
            if tag_entity and entity == 'Organization':
                # TODO: most CorporateBody (and some Place) seem to be given as *range*..
                #rangeclass = graph.resource(LF[entity])
                pass
            else:
                # TODO: when tag_entity and entity, need a rel between them (from tag id?)..
                domainclass = graph.resource(LF[entity])
                domainclass.add(RDF.type, OWL.Class)
        if not domainclass:
            if tag_entity:
                domainclass = graph.resource(LF[tag_entity])
                domainclass.add(RDF.type, OWL.Class)
            else:
                domainclass = LF.Instance # .. or prev? or need to overlay..
        prop = _process_property(col, [domainclass], tag, code, tagdef.get('id'))

def _process_fixprop(tag, baseclass, col, fixprops):
    propRef = col.get('propRef')
    if not propRef or propRef not in fixprops:
        return
    if tag not in fixprop_typerefs or propRef not in fixprop_typerefs[tag]:
        # TODO: create SKOS items?
        #graph.add((propRefEnum, RDFS.subClassOf, SKOS.Concept))
        #fixitem.add(SKOS.broader, propRefEnum)
        return
    propRefEnum = graph.resource(LTAX[upfirst(propRef)])
    propRefEnum.add(RDF.type, OWL.Class)
    propRefEnum.add(RDFS.subClassOf, OWL.Class) # a metaclass
    graph.add((LF[propRef], RDFS.range, propRefEnum.identifier))
    for letter, item in fixprops[propRef].items():
        key = item.get('id')
        if not key or key in ('unspecified', 'unknown', 'other', 'notApplicable', 'noAttemptToCode'):
            continue
        fixitem = graph.resource(LTAX[upfirst(key)])
        fixitem.add(RDF.type, propRefEnum)
        fixitem.add(RDF.type, OWL.Class)
        _add_labels(fixitem, item)
        if baseclass:
            fixitem.add(RDFS.subClassOf, baseclass)

def _process_property(col, domains, tag, code, group=None):
    propRef = col.get('id') or col.get('propRef')
    if not propRef:
        return
    if propRef.endswith('Obsolete'):
        return # TODO: safe guess?
    prop = graph.resource(LF[propRef])
    if col.get('repeatable') is False:
        prop.add(RDF.type, OWL.FunctionalProperty)
    else:
        prop.add(RDF.type, RDF.Property)
    for domain in domains:
        prop.add(DOMAIN, domain)
    _add_labels(prop, col)
    if prop:
        if isinstance(code, dict):
            start = code['offset']
            code = "[%s:%s]" % (start, start + code['length'])
        else:
            code = ".%s" % code
        prop.add(SKOS.notation, Literal(tag + code))
        if group:
            prop.add(VANN.termGroup, LTAX[group])

    return prop

def _add_labels(resource, data):
    for label_key in ('label_sv', 'label_en'):
        label = data.get(label_key)
        if label:
            lang = label_key.split('_', 1)[1]
            resource.add(RDFS.label, Literal(label, lang))

def _fixed_cleanup():
    for rec_prop in graph.subjects(DOMAIN, LF.Record):
        graph.remove((rec_prop, DOMAIN, None))
        graph.add((rec_prop, DOMAIN, LF.Record))

def _cleanup():
    pass # just using lax domains for now..
    #for prop in graph.subjects(DOMAIN):
    #    domains = set(graph.objects(prop, SCHEMA.domain))
    #    if len(domains) > 1:
    #        graph.remove((prop, DOMAIN, None))
    #        union = graph.resource(BNode())
    #        union.add(OWL.unionOf,
    #                Collection(graph, BNode(), sorted(domains)).uri)
    #        graph.add((prop, RDFS.domain, union.identifier))


if __name__ == '__main__':
    from sys import argv, stdout
    if '-d' in argv:
        import cgitb; cgitb.enable(format='text')
    marcmap_path = argv[1]
    with open(marcmap_path) as f:
        marcmap = parse_marcmap(f)
    vocab_from_marcmap(marcmap)
    graph.serialize(stdout, format='n3')
