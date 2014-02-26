from collections import OrderedDict
import json


fpath = "ext-libris/src/main/resources/marcmap.json"
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
    '007': {
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
    },
    '008': {
        'booksContents', #'booksItem', 'booksLiteraryForm' (subjectref), 'booksBiography',
        'computerTypeOfFile',
            #'mapsItem', #'mapsMaterial', # TODO: c.f. 007.mapMaterial
        #'mixedItem',
        #'musicItem', #'musicMatter', 'musicTransposition',
        'serialsTypeOfSerial', #'serialsItem',
        'visualMaterial', #'visualItem',
    }
}

out = OrderedDict()

compositionTypeMap = out['compositionTypeMap'] = OrderedDict()
contentTypeMap = out['contentTypeMap'] = OrderedDict()
carrierTypeMap = out['carrierTypeMap'] = OrderedDict()

fixprops = marcmap['bib']['fixprops']
def to_name(propname, key):
    dfn = fixprops[propname].get(key)
    if dfn and 'id' in dfn:
        name = dfn['id']
        return name[0].upper() + name[1:]
    else:
        return None

for tag, field in sorted(marcmap['bib'].items()):
    if not tag.isdigit():
        continue
    outf = OrderedDict()

    fixmaps = field.get('fixmaps')
    if not fixmaps:
        continue

    tokenTypeMap = OrderedDict()
    for fixmap in fixmaps:
        content_type = None
        if len(fixmaps) > 1:
            outf['tokenTypeMap'] = tokenTypeMap
            type_name = fixmap.get('term') or fixmap['name'].split(tag + '_')[1]
            type_name = entity_map.get(type_name, type_name)
            fm = outf[type_name] = OrderedDict()

            if tag == '008':
                for combo in fixmap['matchRecTypeBibLevel']:
                    rt, bl = combo
                    subtype_name = to_name('typeOfRecord', rt)
                    if not subtype_name:
                        continue
                    comp_name = to_name('bibLevel', bl)
                    if not comp_name:
                        continue

                    is_serial = type_name == 'Serial'
                    if comp_name == 'MonographItem' or is_serial:
                        content_type = contentTypeMap.setdefault(type_name, OrderedDict())
                        subtypes = content_type.setdefault('subclasses', OrderedDict())
                        if is_serial:
                            subtype_name += 'Serial'
                        typedef = subtypes[subtype_name] = OrderedDict()
                        typedef['typeOfRecord'] = rt
                    else:
                        comp_type = compositionTypeMap.setdefault(comp_name, OrderedDict())
                        comp_type['bibLevel'] = bl
                        parts = comp_type.setdefault('partRange', set())
                        parts.add(type_name)
                        parts.add(subtype_name)

            else:
                for k in fixmap['matchKeys']:
                    tokenTypeMap[k] = type_name
        else:
            fm = outf

        for col in fixmap['columns']:
            off, length = col['offset'], col['length']
            key = (#str(off) if length == 1 else
                    '[%s:%s]' % (off, off+length))
            fm[key] = {'key': col.get('propRef', col.get('propId'))}
            dfn_key = col.get('propRef')

            if dfn_key in fixprop_typerefs['007']:
                typemap = carrierTypeMap.setdefault(type_name, OrderedDict())
            elif dfn_key in fixprop_typerefs['008'] and content_type:
                typemap = content_type.setdefault('formClasses', OrderedDict())
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


prevranges = None
for k, v in compositionTypeMap.items():
    ranges = v['partRange']
    if prevranges and ranges - prevranges:
        print "differs", k
    else:
        assert any(r in contentTypeMap for r in ranges)
        v.pop('partRange')
    prevranges = ranges


class SetEncoder(json.JSONEncoder):
   def default(self, obj):
     return list(obj) if isinstance(obj, set) else super(SetEncoder, self).default(obj)


import sys

if '-d' in sys.argv[1:]:
    print json.dumps(out,
            cls=SetEncoder,
            indent=2,
            ensure_ascii=False,
            separators=(',', ': ')
            ).encode('utf-8')

else:
    from rdflib import *

    BASE = Namespace("http://libris.kb.se/def/terms#")

    g = Graph()
    g.bind('', BASE)
    g.bind('owl', OWL)

    def newclass(name, *bases):
        if ' ' in name:
            rtype = g.resource(BNode())
            rtype.add(RDFS.label, Literal(name, lang='sv'))
        else:
            if name == 'Indexes':
                name = 'Index'
            elif name == 'Theses':
                name = 'Thesis'
            elif 'And' in name:
                name = 'Or'.join(s[0:-1] if s.endswith('s') else s
                        for s in name.split('And'))
            elif name.endswith(('Atlas', 'Series')):
                pass
            elif name.endswith('ies'):
                name = name[0:-3] + 'y'
            elif name.endswith('s'):
                name = name[0:-1]
            rtype = g.resource(URIRef(BASE[name]))
        rtype.add(RDF.type, OWL.Class)
        for base in bases:
            rtype.add(RDFS.subClassOf, base)
        return rtype

    SKIP = ('Other', 'Unspecified', 'Unknown')

    for k, v in compositionTypeMap.items():
        rtype = newclass(k, BASE.Part if k.endswith('Part') else BASE.Composite)

    for k, v in contentTypeMap.items():
        rtype = newclass(k, BASE.Work)
        for sk in v['subclasses']:
            if sk in SKIP: continue
            if 'Obsolete' in sk: continue
            stype = newclass(sk, rtype, BASE.Work)
        for sk in v.get('formClasses', ()):
            if sk in SKIP: continue
            stype = newclass(sk, rtype, BASE.Instance)

    for k, v in carrierTypeMap.items():
        rtype = newclass(k, BASE.Instance)
        for sk in v:
            if sk in SKIP: continue
            stype = newclass(sk, rtype, BASE.Carrier)

    g.serialize(sys.stdout, format='turtle')
