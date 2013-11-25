import json

def get_terms(marcframe):
    terms = set()
    for k, v in marcframe['entityTypeMap'].items():
        terms.add(k)
        terms.update(v.get('instanceTypes', []))
    dfn_keys = {'property', 'addProperty', 'link', 'addLink', 'domainEntity', 'rangeEntity'}
    def add_terms(dfn):
        for k, v in dfn.items():
            if k in dfn_keys and v:
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
    args = sys.argv[1:]
    source = args.pop(0)
    context_source = args.pop(0) if args else None

    with open(source) as fp:
        marcframe = json.load(fp)

    terms = get_terms(marcframe)

    if context_source:
        with open(context_source) as fp:
            contexts = json.load(fp)['@context']
            if not isinstance(contexts, list):
                contexts = [contexts]
        context_terms = {term for defs in contexts for term in defs if term[0] != '@'}
        print "# terms missing in context:"
        terms -= context_terms

    for term in sorted(terms):
        print term
