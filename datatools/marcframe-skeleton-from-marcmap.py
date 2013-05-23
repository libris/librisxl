from collections import OrderedDict
import json


fpath = "whelk-extensions/src/main/resources/marcmap.json"
with open(fpath) as f:
    marcmap = json.load(f)

entity_map = {
    'Books': 'Book',
    'Maps': 'Map',
    'Serials': 'Serial',
    'Computer': 'Digital',
    'ComputerFile': 'Digital'
}

fixprop_typerefs = {
    '007': [
        'mapMaterial',
        'computerMaterial',
        'globeMaterial',
        'tacMaterial',
        'projGraphMaterial',
        'microformMaterial',
        'nonProjMaterial',
        'motionPicMaterial',
        'kitMaterial',
        'notatedMusicMaterial',
        'remoteSensImageMaterial', #'soundKindOfMaterial',
        'soundMaterial',
        'textMaterial',
        'videoMaterial'
    ],
    '008': [
        'booksContents', #'booksItem', 'booksLiteraryForm' (subjectref), 'booksBiography',
        'computerTypeOfFile',
            #'mapsItem', #'mapsMaterial', # TODO: c.f. 007.mapMaterial
        #'mixedItem',
        #'musicItem', #'musicMatter', 'musicTransposition',
        'serialsTypeOfSerial', #'serialsItem',
        'visualMaterial', #'visualItem',
    ]
}

show_repeatable = True

out = OrderedDict()
prevtag, outf = None, None

section = out['bib'] = OrderedDict()
contentTypeMap = section['contentTypeMap'] = OrderedDict()
carrierTypeMap = section['carrierTypeMap'] = OrderedDict()

fixprops = marcmap['bib']['fixprops']
def to_name(propname, key):
    dfn = fixprops[propname].get(key)
    if dfn and 'id' in dfn:
        name = dfn['id']
        return name[0].upper() + name[1:]
    else:
        return "[%s]" % key

for tag, field in sorted(marcmap['bib'].items()):
    if not tag.isdigit():
        continue
    prevf, outf = outf, OrderedDict()
    section[tag] = outf
    outf['entity'] = ""
    fixmaps = field.get('fixmaps')
    subfields = field.get('subfield')
    subtypes = None

    if fixmaps:
        tokenTypeMap = OrderedDict()
        for fixmap in fixmaps:
            if len(fixmaps) > 1:
                outf['tokenTypeMap'] = tokenTypeMap
                entity = fixmap.get('term') or fixmap['name'].split(tag + '_')[1]
                entity = entity_map.get(entity, entity)
                fm = outf[entity] = OrderedDict()
                if tag == '008':
                    for combo in fixmap['matchRecTypeBibLevel']:
                        rt, bl = combo
                        named_type = []
                        named_type.append(to_name('typeOfRecord', rt))
                        named_type.append(to_name('bibLevel', bl))
                        content_type = contentTypeMap.setdefault(entity, OrderedDict())
                        subtypes = content_type.setdefault('subtypes', OrderedDict())
                        typedef = subtypes['-'.join(named_type)] = OrderedDict()
                        typedef['typeOfRecord'] = rt
                        typedef['bibLevel'] = bl
                else:
                    for k in fixmap['matchKeys']:
                        tokenTypeMap[k] = entity
            else:
                fm = outf
            for col in fixmap['columns']:
                off, length = col['offset'], col['length']
                key = (#str(off) if length == 1 else
                        '[%s:%s]' % (off, off+length))
                fm[key] = {'key': col.get('propRef', col.get('propId'))}
                dfn_key = col.get('propRef')
                if dfn_key in fixprop_typerefs['007']:
                    typemap = carrierTypeMap.setdefault(entity, OrderedDict())
                elif dfn_key in fixprop_typerefs['008']:
                    typemap = subtypes
                else:
                    typemap = None
                if typemap is not None:
                    valuemap = fixprops[dfn_key]
                    for key, dfn in valuemap.items():
                        if key in ('_', '|'): continue
                        if 'id' in dfn:
                            v = dfn['id']
                            subname = v[0].upper() + v[1:]
                        else:
                            subname = dfn['label_sv']
                        subdef = typemap[subname] = {dfn_key: key}

    elif not subfields:
        outf['key'] = field['id']

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
            subf['key'] = sid
            if show_repeatable and 'repeatable' in subfield:
                subf['repeatable'] = subfield['repeatable']
        if len(outf.keys()) > 1 and outf == prevf:
            section[tag] = {'inherit': prevtag}
    if show_repeatable and 'repeatable' in field:
        outf['repeatable'] = field['repeatable']
    prevtag = tag


#class SetEncoder(json.JSONEncoder):
#   def default(self, obj):
#      if isinstance(obj, set):
#         return list(obj)
#      return json.JSONEncoder.default(self, obj)

print json.dumps(out,
        #cls=SetEncoder,
        indent=2,
        ensure_ascii=False,
        separators=(',', ': ')
        ).encode('utf-8')
