import json

def describe_marcmap(marcmap):
    for tag, field in sorted(marcmap['bib'].items()):
        if tag == 'fixprops':
            continue
        print tag,_id_or_label(field)
        for fixmap in field.get('fixmaps', ()):
            print " ", fixmap.get('label_en', "").replace('&', '')
            for col in fixmap['columns']:
                print "    {0} ({1}+{2})".format(*[col.get(key) for key in
                        ('propRef', 'offset', 'length')])
        if 'subfield' in field:
            # TODO: ind is a dict, *value* has _id_or_label
            #inds = ", ".join(filter(None, (_id_or_label(field[ind])
            #        for ind in ['ind1', 'ind2'] if ind in field)))
            inds = ", ".join(filter(None, (" ".join(field[ind].keys())
                    for ind in ['ind1', 'ind2'] if ind in field)))
            if inds:
                print " ", inds
            for code, subfield in field['subfield'].items():
                print " ", code, subfield['id']
        print

def _id_or_label(data):
    return data.get('id') or data.get('label_en')


if __name__ == '__main__':
    from sys import argv, exit
    import os
    if len(argv) < 2:
        print "Usage: {0} MARCMAP".format(os.path.basename(argv[0]))
        exit()
    fpath = argv[1]
    with open(fpath) as f:
        describe_marcmap(json.load(f))

