#!/usr/bin/env python3
import json
import sys
import os


IDKBSE_TERM = 'https://id.kb.se/term/'

headers = (
        'ID',
        'type',
        #'scheme',
        'inCollection',
        'prefLabel',
        'broader',
        'related',
        'closeMatch',
        'broadMatch',
        'hasVariant',
        'scopeNote',
        'historyNote',
        'cataloguersNote',
        'sourceConsulted',
        'created',
        #'descriptionCreator',
        'modified',
        #'descriptionLastModifier',
        'XL_ID',
        'sameAs',
        'identifiedBy',
        )


def auth_to_tsv(data, basedir, outfiles):
    graph = data['@graph']
    record, thing = graph[:2]
    work = graph[2] if len(graph) > 2 else None
    schemecode = None
    try:
        scheme_id = thing['inScheme'].get('@id')
        if scheme_id and scheme_id.startswith(IDKBSE_TERM):
            schemecode = scheme_id.rsplit('/', 1)[1]
        else:
            schemecode = 'code-' + thing['inScheme']['code'].replace('/', '-')
    except KeyError:
        pass
    if schemecode:
        if basedir:
            fname = f"{schemecode}-{thing['@type']}.tsv"
            outfile = outfiles.get(fname)
            if not outfile:
                outfile = outfiles[fname] = open(os.path.join(basedir, fname), 'w')
                print(*headers, sep='\t', file=outfile)
        else:
            outfile = sys.stdout
        print(
                thing['@id'].split('/', 3)[-1],
                thing['@type'],
                #schemecode,
                col(thing, 'inCollection'),
                thing.get('prefLabel', '?'),
                col(thing, 'broader'),
                col(thing, 'related'),
                col(thing, 'closeMatch'),
                col(thing, 'broadMatch'),
                col(thing, 'hasVariant'),
                col(thing, 'hasNote', lambda note, owner: note['scopeNote'][0] if set(note.keys()) == {'@type', 'scopeNote'} else json.dumps(note)),
                thing.get('historyNote', ''),
                thing.get('cataloguersNote', ''),
                col(record, 'sourceConsulted'),
                record['created'],
                #record['descriptionCreator']['@id'].split('/', 3)[-1],
                record['modified'],
                #record['descriptionLastModifier']['@id'].split('/', 3)[-1],
                record['@id'].rsplit('/', 1)[1],
                col(record, 'sameAs'),
                col(record, 'identifiedBy'),
                sep='\t', file=outfile)


def col(obj, key, repr_anon=None):
    repr_anon = repr_anon or repr_anon_term
    return ' | '.join(
            it.get('@id', '').replace(IDKBSE_TERM, '') or repr_anon(it, owner=obj)
            for it in obj.get(key, []))


def repr_anon_term(term, owner=None):
    sameas = term.get('sameAs')

    if sameas and len(sameas) == 1:
        return sameas[0]['@id'].replace(IDKBSE_TERM, '')

    #if all(isinstance(v, str) for v in term.values()):
    #    jsonrepr = ' :: '.join(term.values())
    #else:
    jsonrepr = json.dumps(term)

    missing = '???'

    ttype = term.pop('@type')
    if ttype == 'ComplexSubject':
        components = term['termComponentList']
        first_type = components[0]['@type']
        if owner['@type'] == first_type and \
                all(it.get('@type', '').startswith(first_type)
                        for it in components[1:]):
            return '--'.join(it['prefLabel'] for it in components)

    code = term.pop('code', missing)
    label = term.pop('marc:explanatoryTerm', None)

    table = term.pop('marc:tableIdentificationTableNumber', None)
    # TODO: just drop table like this? (╯°□°）╯︵ ┻━┻
    ctrlsubfield = term.pop('marc:controlSubfield', None)
    # TODO: and drop ctrlsubfield just like that?

    if ttype == 'ClassificationDdc':
        prefix = "ddc"
        if 'edition' in term:
            prefix = f"{prefix}/{term.pop('edition')}"
        symbol = f"{prefix}/{code}"
    elif ttype == 'Classification':
        scheme = term.pop('inScheme', None)
        if not scheme:
            prefix = missing
        else:
            prefix = scheme.get('@id') or f"{scheme.get('code')}/{scheme.get('version')}"
        symbol = f"{prefix}/{code}"
    else:
        symbol = None

    if symbol and not len(term):
        if label:
            symbol = f"{symbol} ({label})"
        return symbol
    else:
        scheme = term.pop('inScheme', None) if len(term) == 2 else None
        if len(term) == 1:
            label = term.get('prefLabel') or term.get('label')
            if label and owner.get('@type') == ttype:
                if scheme:
                    prefix = scheme['@id'].split('/', 2)[-1] \
                            if '@id' in scheme else scheme['code']
                    return f"{prefix}/{label}"
                return label
        return jsonrepr


def main(basedir):
    outfiles = {}

    for i, l in enumerate(sys.stdin):
        if not l.rstrip():
            continue
        l = l.replace('\\\\"', '\\"')
        try:
            data = json.loads(l)
            auth_to_tsv(data, basedir, outfiles)
        except ValueError as e:
            print(f"ERROR at {i} in data:\n{l}; error: {e}", file=sys.stderr)

    for f in outfiles.values():
        f.close()


if __name__ == '__main__':
    basedir = sys.argv[1] if len(sys.argv) > 1 else '/tmp/'
    if not os.path.isdir(basedir):
        os.makedirs(basedir)
    main(basedir)
