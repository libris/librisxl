import json
from operator import itemgetter as iget

def packaged(data, known=None):
    for o in data.values():
        for mtype, typemapping in o.items():
            props = typemapping['properties']
            if '_marcUncompleted' not in props:
                continue
            uncompleted = props['_marcUncompleted']['properties']
            def get_fields():
                for tag, mapping in uncompleted.items():
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
        for field, subfields in sorted(fields, key=iget(0)):
            print field, ", ".join(subfields) if subfields else "(FIXED)"
        print

def to_facet_query(data, for_mtype, known=None, indent="  "):
    for mtype, fields in packaged(data, known):
        if mtype != for_mtype:
            continue
        for field, subfields in fields:
            if not subfields:
                continue
            qs = '"_marcUncompleted.%(field)s.%(subfield)s": { "terms": {"size": 5, "field": "_marcUncompleted.%(field)s.subfields.%(subfield)s"}}'
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

def query_results_to_tsv(data):
    rows = ((key.split('.', 1)[1], stats['total'], stats['terms'])
            for key, stats in data['facets'].items())

    for key, total, values in sorted(rows, key=iget(1), reverse=True):
        tag, code = key.split('.')
        vals = (u", ".join("%(term)s (%(count)s)" % v for v in values))
        print (u"%s\t%s\t%s\t%s" % (tag, code, total, vals)).encode('utf-8')


if __name__ == '__main__':
    from optparse import OptionParser
    op = OptionParser()
    op.add_option('-f', '--facet-for-type', type=str, help="Generate facet query for type")
    op.add_option('-s', '--es-server', type=str, help="name:port of elasticsearch server")
    op.add_option('-t', '--make-tsv', action='store_true', help="Digest query results into TSV data")
    opts, args = op.parse_args()

    if args:
        fpath = args.pop(0)
        with open(fpath) as fp:
            data = json.load(fp)
    for k in data:
        if k.startswith('libris-'):
            data = data[k]
        break

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
      "fields": ["_marcUncompleted"]
    }' | python -mjson.tool"""
    #    "query": {"term": {"field": "value"}}

    if opts.facet_for_type:
        facet_query = ",\n".join(to_facet_query(data, opts.facet_for_type, known))
        print cmd_str  % (opts.es_server, opts.facet_for_type, facet_query)
    elif opts.make_tsv:
        import sys
        query_results_to_tsv(json.load(sys.stdin))
    else:
        print_summary(data, known)
