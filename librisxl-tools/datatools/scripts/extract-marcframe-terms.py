import json


def get_keywords(marcframe, ignore_speced=True):
    keywords = set()
    speced = set()
    for name in 'auth', 'bib', 'hold':
        part = marcframe.get(name, {})
        for field in part.values():
            if not field:
                continue
            is_speced = any(kw in field for kw in ['_specSource', '_spec'])
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
                                if is_speced:
                                    speced |= set(subobj)
                            else:
                                keywords.add(subkw)
                                if is_speced:
                                    speced.add(kw)
                elif not kw.startswith('['):
                    keywords.add(kw)
                    if is_speced:
                        speced.add(kw)
    if ignore_speced:
        return keywords - speced
    return keywords


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    source = args.pop(0)

    with open(source) as fp:
        marcframe = json.load(fp)

    keywords = get_keywords(marcframe)
    for kw in sorted(keywords):
        print kw
