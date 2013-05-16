from collections import OrderedDict
import json


fpath = "whelk-extensions/src/main/resources/marcmap.json"
with open(fpath) as f:
    marcmap = json.load(f)

show_repeatable = True

out = OrderedDict()
prevtag, outf = None, None

for tag, field in sorted(marcmap['bib'].items()):
    if not tag.isdigit():
        continue
    prevf, outf = outf, OrderedDict()
    out[tag] = outf
    outf['entity'] = ""
    fixmaps = field.get('fixmaps')
    subfields = field.get('subfield')
    if fixmaps:
        for fixmap in fixmaps:
            fm = outf[fixmap['name']] = OrderedDict()
            for col in fixmap['columns']:
                off, length = col['offset'], col['length']
                key = (#str(off) if length == 1 else
                        '[%s:%s]' % (off, off+length))
                fm[key] = {'id': col.get('propRef', col.get('propId'))}
    elif not subfields:
        outf['id'] = field['id']
    else:
        for ind_key in ('ind1', 'ind2'):
            ind = field.get(ind_key)
            if not ind:
                continue
            ind_keys = sorted(k for k in ind if k != '_')
            if ind_keys:
                outf[ind_key.replace('ind', 'i')] = {}
        for code, subfield in subfields.items():
            sid = subfield.get('id') or ""
            if sid.endswith('Obsolete'):
                outf['obsolete'] = True
                sid = sid[0:-8]
            if (code, sid) in [('6', 'linkage'), ('8', 'fieldLinkAndSequenceNumber')]:
                continue
            subf = outf['$' + code] = OrderedDict()
            subf['id'] = sid
            if show_repeatable and 'repeatable' in subfield:
                subf['repeatable'] = subfield['repeatable']
        if len(outf.keys()) > 1 and outf == prevf:
            out[tag] = {'inheritOverlayFrom': prevtag}
    if show_repeatable and 'repeatable' in field:
        outf['repeatable'] = field['repeatable']
    prevtag = tag


print json.dumps(out,
        indent=2,
        ensure_ascii=False,
        separators=(',', ': ')
        ).encode('utf-8')
