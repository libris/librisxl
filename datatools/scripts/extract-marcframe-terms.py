import json


def get_keywords(marcframe):
    keywords = set()
    for name in 'auth', 'bib', 'hold':
        part = marcframe.get(name, {})
        for field in part.values():
            if not field:
                continue
            for kw, obj in field.items():
                if 'NOTE:' in kw.upper() or 'TODO' in kw.upper():
                    continue
                if '$' in kw or kw[0].isupper():
                    if obj:
                        if not isinstance(obj, dict):
                            continue
                        for subkw, subobj in obj.items():
                            if subkw.startswith('['):
                                keywords |= set(subobj)
                            else:
                                keywords.add(subkw)
                elif not kw.startswith('['):
                    keywords.add(kw)
    return keywords


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    source = args.pop(0)

    with open(source) as fp:
        marcframe = json.load(fp)

    for kw in sorted(get_keywords(marcframe)):
        print kw
