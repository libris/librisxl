import json

def packaged(data):
    for o in data.values():
        for mtype, typemapping in o.items():
            props = typemapping['properties']
            if 'unknown' not in props:
                continue
            unknown = props['unknown']['properties']
            def get_fields():
                for field, mapping in unknown.items():
                    if 'properties' not in mapping:
                        subfields = None
                    else:
                        subfields = mapping['properties']['subfields']['properties'].keys()
                    yield field, subfields
            yield mtype, get_fields()

def print_summary(data):
    for mtype, fields in packaged(data):
        print '#', mtype
        for field, subfields in fields:
            print field, ", ".join(subfields) if subfields else "(FIXED)"
        print

def print_facet_query(data):
    for mtype, fields in packaged(data):
        print '//', mtype
        for field, subfields in fields:
            if not subfields:
                continue
            qs = '"unknown.%(field)s.%(subfield)s": { "terms": {"size": 5, "field": "unknown.%(field)s.subfields.%(subfield)s"}}'
            for subfield in subfields:
                print " ", qs % vars(), ','
        print

if __name__ == '__main__':
    from sys import argv
    args = argv[1:]
    fpath = args.pop(0)
    with open(fpath) as fp:
        data = json.load(fp)
    if '-f' in args:
        print_facet_query(data)
    else:
        print_summary(data)
