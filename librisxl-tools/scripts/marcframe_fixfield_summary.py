import json

with open('whelk-core/src/main/resources/ext/marcframe.json') as f:
  marcframe = json.load(f)

for fixfield in ['007', '008', '006']:
    fixfield_dfn = marcframe['bib'][fixfield]

    for key, dfn in fixfield_dfn.items():
        if not isinstance(dfn, dict):
            continue
        for subkey, subdfn in dfn.items():
            if not isinstance(subdfn, dict):
                continue
            link = subdfn.get('link') or subdfn.get('addLink')
            if link:
                if subkey[0] != '[' or subkey[-1] != ']':
                    continue
                cols = subkey
                marcsrc = f"bib-{fixfield}{cols}"
                enumtype = subdfn.get('uriTemplate')
                if enumtype:
                    enumtype = enumtype.removeprefix("https://id.kb.se/marc/")
                    enumtype = enumtype.removesuffix("-{_}")
                tokens = subdfn.get('matchUriToken')
                if tokens:
                    tokens = ' '.join(tokens.removeprefix('^[').removesuffix(']$'))
                print(key, marcsrc, link, enumtype or subdfn.get('resourceType'), tokens, sep='\t')
