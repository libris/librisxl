import json

def packaged(data, known=None):
    for o in data.values():
        for mtype, typemapping in o.items():
            props = typemapping['properties']
            if 'unknown' not in props:
                continue
            unknown = props['unknown']['properties']
            def get_fields():
                for tag, mapping in unknown.items():
                    if 'properties' not in mapping:
                        subfields = None
                    else:
                        subfields = mapping['properties']['subfields']['properties'].keys()
                        if known:
                            subfields = sorted(sf for sf in subfields
                                    if sf not in known[mtype].get(tag, []))
                    yield tag, subfields
            yield mtype, get_fields()

def print_summary(data, known=None):
    for mtype, fields in packaged(data, known):
        print '#', mtype
        for field, subfields in sorted(fields, key=lambda t: t[0]):
            print field, ", ".join(subfields) if subfields else "(FIXED)"
        print

def to_facet_query(data, for_mtype, known=None, indent="  "):
    for mtype, fields in packaged(data, known):
        if mtype != for_mtype:
            continue
        for field, subfields in fields:
            if not subfields:
                continue
            qs = '"unknown.%(field)s.%(subfield)s": { "terms": {"size": 5, "field": "unknown.%(field)s.subfields.%(subfield)s"}}'
            for subfield in subfields:
                yield indent + (qs % vars())

def _get_known(marcframe):
    def merged(dfn, tag, mdefs):
        ref = dfn.get('inherit')
        if not ref:
            return dfn
        ref_tag = tag
        if ':' in ref:
            ref, ref_tag = ref.split(':')
        base = mdefs[ref] if ref in mdefs else marcframe[ref][ref_tag]
        if 'inherit' in base:
            mdefs = marcframe[ref] if ref in marcframe else mdefs
            base = merged(base, ref or ref_tag, mdefs)
        mdfn = dict(base, **dfn)
        mdfn.pop('inherit')
        return mdfn
    return {mtype: {
                tag: [k[1:] for k in merged(dfn, tag, marcframe[mtype]) if k.startswith('$')]
                for tag, dfn in marcframe[mtype].items()
            } for mtype in ('bib', 'auth', 'hold')}

if __name__ == '__main__':
    from optparse import OptionParser
    op = OptionParser()
    op.add_option('-f', '--facet-for-type', type=str, help="Generate facet query for type")
    op.add_option('-s', '--es-server', type=str, help="name:port of elasticsearch server")
    opts, args = op.parse_args()

    fpath = args.pop(0)
    with open(fpath) as fp:
        data = json.load(fp)

    if args:
        fpath = args.pop(0)
        with open(fpath) as fp:
            marcframe = json.load(fp)
            known = _get_known(marcframe)
    else:
        known = {}

    cmd_str = """curl -s -XGET http://%s/libris/%s/_search -d '{
      "size": 0,
      "facets" : {
        %s
      },
      "fields": ["unknown"]
    }' | python -mjson.tool"""
    #    "query": {"term": {"field": "value"}}

    if opts.facet_for_type:
        facet_query = ",\n".join(to_facet_query(data, opts.facet_for_type, known))
        print cmd_str  % (opts.es_server, opts.facet_for_type, facet_query)
    else:
        print_summary(data, known)
