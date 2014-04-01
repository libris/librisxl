import json
from rdflib import *
from rdflib.util import guess_format

SDO = Namespace("http://schema.org/")
VANN = Namespace("http://purl.org/vocab/vann/")

TERMS = Namespace("http://libris.kb.se/def/terms#")

ENUM_BASE = "http://libris.kb.se/def/enum/"
ENUM_MAP = {
    'tcon': Namespace(ENUM_BASE + 'content/'),
    'tcar': Namespace(ENUM_BASE + 'carrier/'),
    'trec': Namespace(ENUM_BASE + 'record/')
}

SKIP = ('Other', 'Unspecified', 'Unknown')


def parse_marcframe(marcframe):
    g = Graph()
    g.bind('', TERMS)
    g.bind('owl', OWL)
    g.bind('sdo', SDO)

    # IMP: dataset = ConjunctiveGraph() with subgraph per enumeration?

    for part in ['bib', 'auth', 'hold']:
        parse_resourcemap(g, part, marcframe)

        for field in marcframe[part].values():
            add_terms(g, field)

    return g


def parse_resourcemap(g, part, marcframe):
    if part != 'bib':
        return # TODO

    basegroup_map = {}

    def parse_fixed(tag, link, baseclass, groupname):
        for basename, coldefs in marcframe[part][tag].items():
            if not basename[0].isupper():
                continue
            rclass = newclass(g, basename, baseclass, groupname)
            for coldfn in coldefs.values():
                if coldfn.get('addLink') == link:
                    map_key = coldfn['tokenMap']
                    basegroup_map[map_key] = rclass, groupname

    parse_fixed('008', 'contentType', TERMS.CreativeWork, "content")
    parse_fixed('007', 'carrierType', TERMS.Product, "carrier")

    carrier_type_base_map = {}

    for mapname, dfn in marcframe['tokenMaps'].items():
        if mapname == 'typeOfRecord':
            for name in dfn.values():
                rtype = newclass(g, name, None, "typeOfRecord")
        if mapname == 'bibLevel':
            for name in dfn.values():
                if not name:
                    continue
                if name.lower().endswith(('part', 'unit')):
                    base = TERMS.Part
                elif name in ('MonographItem',
                        'AdministrativePostForLicenseBoundElectronicResource'):
                    base = None
                else:
                    base = TERMS.Aggregate
                if base:
                    rtype = newclass(g, name, base, "bibLevel")
        else:
            basegroup = basegroup_map.get(mapname)

            if not basegroup:
                continue

            for sk in dfn.values():
                if not sk or sk in SKIP or 'Obsolete' in sk:
                    continue
                # TODO: use ENUMS for name?
                stype = newclass(g, sk, *basegroup)



def add_terms(g, dfn, domainname=None):
    for k, v in dfn.items():
        if not v:
            continue
        if k == 'defaults':
            for dp in v:
                newprop(g, dp, {RDF.Property})
            continue

        rtypes = {
            'property': {OWL.DatatypeProperty, OWL.FunctionalProperty},
            'addProperty': {OWL.DatatypeProperty},
            'link': {OWL.ObjectProperty, OWL.FunctionalProperty},
            'addLink': {OWL.ObjectProperty},
        }.get(k)

        domainname = dfn.get('domainEntity', domainname)
        rangename = dfn.get('rangeEntity')

        is_link = ('link' in dfn or 'addLink' in dfn)

        if k == 'property' and is_link:
            domainname, rangename = rangename, None

        if not rtypes:
            if not isinstance(v, list):
                v = [v]
            for subdfn in v:
                if isinstance(subdfn, dict):
                    add_terms(g, subdfn, rangename if is_link else None)
            continue

        newprop(g, v, rtypes, domainname, rangename)


def newclass(g, name, base=None, termgroup=None):
    if ' ' in name:
        rclass = g.resource(BNode())
        rclass.add(RDFS.label, Literal(name, lang='sv'))
    elif ':' in name:
        token, name = name.split(':', 1)
        rclass = g.resource(URIRef(ENUM_MAP[token][name]))
    else:
        rclass = g.resource(URIRef(TERMS[name]))
    rclass.add(RDF.type, OWL.Class)
    if base:
        rclass.add(RDFS.subClassOf, base)
    if termgroup:
        rclass.add(VANN.termGroup, Literal(termgroup))#ENUM[termgroup])
    return rclass

def newprop(g, name, rtypes, domainname=None, rangename=None):
    if not name or name in ('@id', '@type'):
        return
    rprop = g.resource(URIRef(TERMS[name]))
    for rtype in rtypes:
        rprop.add(RDF.type, rtype)
    if domainname:
        rprop.add(SDO.domainIncludes, TERMS[domainname])
    if rangename:
        rprop.add(SDO.rangeIncludes, TERMS[rangename])
    return rprop


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    source = args.pop(0)
    termspath = args.pop(0) if args else None

    with open(source) as fp:
        marcframe = json.load(fp)

    g = parse_marcframe(marcframe)

    if termspath:
        tg = Graph().parse(termspath, format=guess_format(termspath))
        g -= tg
        g.remove((None, VANN.termGroup, None))

    g.serialize(sys.stdout, format='turtle')
