import json
from urlparse import urljoin
from rdflib import *
from rdflib.util import guess_format

SDO = Namespace("http://schema.org/")
VANN = Namespace("http://purl.org/vocab/vann/")

BASE = "http://libris.kb.se/"
TERMS = Namespace(BASE + "def/terms#")
ENUM_BASEPATH = "/def/enum/"

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



def add_terms(g, dfn, parentdomain=None):

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

        is_link = ('link' in dfn or 'addLink' in dfn)

        key_is_property = k in ('property', 'addProperty')

        if key_is_property and is_link:
            domainname = dfn.get('rangeEntity')
            rangename = None
        else:
            domainname = dfn.get('domainEntity', parentdomain)
            rangename = dfn.get('rangeEntity')

        if not rtypes:
            if not isinstance(v, list):
                v = [v]
            for subdfn in v:
                if isinstance(subdfn, dict):
                    subdomainname = (rangename if
                            is_link and not key_is_property else None)
                    add_terms(g, subdfn, subdomainname)
            continue

        newprop(g, v, rtypes, domainname, rangename)


def newclass(g, name, base=None, termgroup=None):
    if ' ' in name:
        rclass = g.resource(BNode())
        rclass.add(RDFS.label, Literal(name, lang='sv'))
    elif name.startswith(ENUM_BASEPATH):
        rclass = g.resource(URIRef(urljoin(BASE, name)))
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


def add_equivalent(g, refgraph):
    for s in refgraph.subjects():
        try:
            qname = refgraph.qname(s)
        except:
            continue
        if ':' in qname:
            qname = qname.split(':')[-1]
        term = TERMS[qname]
        if (term, None, s) not in g:
            if (term, RDF.type, OWL.Class) in g:
                rel = OWL.equivalentClass
            elif (term, None, None) in g:
                rel = OWL.equivalentProperty
            else:
                continue
            g.add((term, rel, s))


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    source = args.pop(0)
    termspath = args.pop(0) if args else None

    with open(source) as fp:
        marcframe = json.load(fp)

    g = parse_marcframe(marcframe)

    for refpath in args:
        refgraph = Graph().parse(refpath, format=guess_format(refpath))
        add_equivalent(g, refgraph)

    if termspath:
        tg = Graph().parse(termspath, format=guess_format(termspath))
        g -= tg
        g.remove((None, VANN.termGroup, None))
        g.namespace_manager = tg.namespace_manager

    g.serialize(sys.stdout, format='turtle')
