from collections import OrderedDict
import json
from rdflib import *

BF = Namespace("http://bibframe.org/vocab/")
ABS = Namespace("http://bibframe.org/model-abstract/")

src = "/Users/nlm/Documents/rdfmodels/bibframe.ttl"

g = Graph().parse(src, format="turtle")
g.namespace_manager.bind("", BF)

out = {}

def add_value_or_list(d, k, v):
    current = d.get(k)
    if current == v:
        pass
    elif current:
        if not isinstance(current, list):
            d[k] = [current]
        d[k].append(v)
    else:
        d[k] = v

for t in (RDFS.Class, ABS.BFProperty):
    for r in sorted(map(g.resource, g.subjects(RDF.type, t))):
        term = r.qname()
        marc = ",".join(r.objects(ABS.marcField))
        if not marc:
            continue
        if t == RDFS.Class:
            if marc.startswith("Ldr"):
                # TODO: type determined by a combo of Ldr and some other fields (00[6-8]) + 33*
                continue
            tags = [s.strip()[0:3] for s in marc.split(",")]

            main_types = (BF.Work, BF.Instance)
            is_main_type = r.identifier in main_types or any(c.identifier in main_types
                    for c in r.transitive_objects(RDFS.subClassOf))
            entity_role = 'domainEntity' if is_main_type else 'rangeEntity'
            for tag in tags:
                field = out.setdefault(tag, {})
                add_value_or_list(field, entity_role, term)
        else:
            if any(s in marc for s in ['=', '/', '??', 'if']):
                # TODO
                #print "Cannot parse condition:", marc
                continue
            if " " in marc:
                tags, subcodes = marc.split(" ", 1)
                subcodes = subcodes.split(",")
            else:
                tags, subcodes = marc, []
            tags = [s for s in tags.split(",") if s and s.isdigit()]
            if all(len(c) == 3 for c in subcodes):
                tags += subcodes
                subcodes = []
            ranges = list(r.objects(RDFS.range))
            objranges = [rng for rng in ranges if rng.identifier.startswith(BF)]
            for tag in sorted(tags):
                field = out.setdefault(tag, {})
                if subcodes:
                    for code in sorted(subcodes):
                        if code[0] == '$':
                            code = code[1:]
                        if code in "/+" or len(code) > 1:
                            # TODO
                            #print "unhandled:", subcodes
                            continue
                        subf = field.setdefault('$'+ code, {})
                        add_value_or_list(subf, 'id', term)
                if not ranges or objranges:
                    add_value_or_list(field, 'link', term)
                    if objranges:
                        add_value_or_list(field, 'rangeEntity', objranges[0].qname())

def sortdict(o):
    if not isinstance(o, dict):
        return o
    return OrderedDict((k, sortdict(o[k])) for k in sorted(o))

out = sortdict(out)

print json.dumps(out,
        indent=2,
        ensure_ascii=False,
        separators=(',', ': ')
        ).encode('utf-8')
