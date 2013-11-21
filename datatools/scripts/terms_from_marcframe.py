import json

def get_terms(marcframe):
    terms = set()
    for k, v in marcframe['entityTypeMap'].items():
        terms.add(k)
        terms.update(v.get('instanceTypes', []))
    dfn_keys = {'property', 'addProperty', 'link', 'addLink', 'domainEntity', 'rangeEntity'}
    def add_terms(dfn):
        for k, v in dfn.items():
            if k in dfn_keys:
                terms.add(v)
            elif isinstance(v, dict):
                add_terms(v)
            elif k == 'defaults':
                terms.update(v)
    for part in ['bib', 'auth', 'hold']:
        for field in marcframe[part].values():
            add_terms(field)
    return terms

if __name__ == '__main__':
    import sys
    source = sys.argv[1]
    with open(source) as fp:
        marcframe = json.load(fp)
    terms = get_terms(marcframe)
    for term in sorted(terms):
        print term
