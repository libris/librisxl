from collections import OrderedDict
import json

import sys

args = sys.argv[1:]
fpath = args.pop(0) if args else "ext-libris/src/main/resources/marcmap.json"
with open(fpath) as f:
    marcmap = json.load(f)

include_lang = '-l' in args
show_repeatable = '-R' not in args
skip_unlinked_maps = '-l' in args


content_name_map = {
    'Books': 'Text',
    'Book': 'Text',
    'Maps': 'Cartography',
    'Map': 'Cartography',
    'Music': 'Audio',
    'Serials': 'Serial',
    'Computer': 'Digital'
}

carrier_name_map = {
    'ComputerFile': 'Electronic',
    'ProjectedGraphic': 'ProjectedImage',
    'NonprojectedGraphic': 'StillImage',
    'MotionPicture': 'MovingImage',
}

propname_map = {
    'item': 'additionalCarrierType',
    'format': 'additionalType'
}

fixprop_typerefs = {
    '000': [
        'typeOfRecord',
        'bibLevel',
    ],
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
    ],
    '006': [
        'booksContents',
        'computerTypeOfFile',
        'serialsTypeOfSerial',
        'visualMaterial',
    ]
}
# TODO: literaryForm => genre (same as 655)


common_columns = {
    '008': {"[0:6]", "[6:7]", "[7:11]", "[11:15]", "[15:18]", "[35:38]", "[38:39]", "[39:40]"}
}

def to_name(name):
    name = name[0].upper() + name[1:]
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
    return name


fixprops = marcmap['bib']['fixprops']
def fixprop_to_name(propname, key):
    dfn = fixprops[propname].get(key)
    if dfn and 'id' in dfn:
        return to_name(dfn['id'])
    else:
        return None

out = OrderedDict()

if include_lang:
    out["@context"] = ["/def/context/skos.jsonld",
        {"enumDefs": {"@id": "@graph", "@container": "@index"},
          "@language": "sv",
          "tokenMaps": None, "bib": None}]
    enum_defs = out['enumDefs'] = OrderedDict()

tokenMaps = out['tokenMaps'] = OrderedDict()
#compositionTypeMap = out['compositionTypeMap'] = OrderedDict()
#contentTypeMap = out['contentTypeMap'] = OrderedDict()

tmap_hashes = {} # to reuse repeated tokenmap
enums = set() # to prevent enum id collisions

def get_tokenmap_key(name, items):
    mhash = hash(tuple(items))
    ref = tmap_hashes.get(mhash)
    if ref:
        return ref
    else:
        tmap_hashes[mhash] = name
        return name

def find_boolean_off_key(tokenmap):
    if tokenmap and len(tokenmap) == 2:
        items = [(k, v.split(':', 1)[-1] if v else '_')
                    for (k, v) in tokenmap.items()]
        for off_index, (k, v) in enumerate(items):
            if v.startswith('No'):
                on_k, on_v = items[not off_index]
                if v.endswith(on_v.replace('Present', '')):
                    assert k == '0' and on_k == '1'
                    return k
                break

def add_labels(src, dest):
    sv = src.get('label_sv')
    en = src.get('label_en')
    #if sv or en:
    #    lang = dest['label'] = {}
    #    if sv: lang['sv'] = sv
    #    if en: lang['en'] = en
    if sv:
        dest['prefLabel'] = sv
    if en:
        dest['prefLabel_en'] = en


section = out['bib'] = OrderedDict()

prevtag, outf = None, None
for tag, field in sorted(marcmap['bib'].items()):
    if not tag.isdigit():
        continue
    prevf, outf = outf, OrderedDict()
    section[tag] = outf
    fixmaps = field.get('fixmaps')
    subfields = field.get('subfield')
    subtypes = None

    if fixmaps:
        tokenTypeMap = OrderedDict()
        for fixmap in fixmaps:
            content_type = None
            orig_type_name = None
            if len(fixmaps) > 1:
                if tag == '008':
                    rt_bl_map = outf.setdefault('recTypeBibLevelMap', OrderedDict())
                else:
                    outf['addLink'] = 'hasFormat' if tag == '007' else 'hasPart'
                    outf['[0:1]'] = {
                        'addProperty': '@type',
                        'tokenMap': tokenTypeMap,
                    }
                    outf['tokenTypeMap'] = '[0:1]'

                orig_type_name =  fixmap['name'].split(tag + '_')[1]
                type_name = fixmap.get('term') or orig_type_name
                if tag in ('006', '008'):
                    type_name = content_name_map.get(type_name, type_name)
                elif tag in ('007'):
                    type_name = carrier_name_map.get(type_name, type_name)

                fm = outf[type_name] = OrderedDict()
                if include_lang:
                    add_labels(fixmap, fm)

                if tag == '008':
                    for combo in fixmap['matchRecTypeBibLevel']:
                        rt_bl_map[combo] = type_name
                        rt, bl = combo
                        subtype_name = fixprop_to_name('typeOfRecord', rt)
                        if not subtype_name:
                            continue
                        comp_name = fixprop_to_name('bibLevel', bl)
                        if not comp_name:
                            continue

                        #is_serial = type_name == 'Serial'
                        #if comp_name == 'MonographItem' or is_serial:
                        #    content_type = contentTypeMap.setdefault(type_name, OrderedDict())
                        #    subtypes = content_type.setdefault('subclasses', OrderedDict())
                        #    if is_serial:
                        #        subtype_name += 'Serial'
                        #    typedef = subtypes[subtype_name] = OrderedDict()
                        #    typedef['typeOfRecord'] = rt
                        #else:
                        #    comp_type = compositionTypeMap.setdefault(comp_name, OrderedDict())
                        #    comp_type['bibLevel'] = bl
                        #    parts = comp_type.setdefault('partRange', set())
                        #    parts.add(type_name)
                        #    parts.add(subtype_name)

                else:
                    for k in fixmap['matchKeys']:
                        tokenTypeMap[k] = type_name
            else:
                fm = outf

            #local_enums = set()
            real_fm = fm
            for col in fixmap['columns']:
                off, length = col['offset'], col['length']
                key = (#str(off) if length == 1 else
                        '[%s:%s]' % (off, off+length))
                if key in common_columns.get(tag, ()):
                    # IMP: verify expected props?
                    fm = outf
                else:
                    fm = real_fm

                dfn_key = col.get('propRef')
                if not dfn_key:
                    continue
                if orig_type_name == 'ComputerFile':
                    orig_type_name = 'Computer'
                if orig_type_name and dfn_key.title().startswith(orig_type_name):
                    new_propname = dfn_key[len(orig_type_name):]
                    new_propname = new_propname[0].lower() + new_propname[1:]
                    #print new_propname, '<-', dfn_key
                else:
                    new_propname = None

                fm[key] = col_dfn = OrderedDict()
                propname = new_propname or dfn_key or col.get('propId')
                if propname in propname_map:
                    propname = propname_map[propname]
                domainname = 'Record' if tag == '000' else None
                is_link = False
                repeat = False
                valuemap = None
                tokenmap = None
                if include_lang:
                    add_labels(col, col_dfn)

                if dfn_key in fixprops:
                    valuemap = fixprops[dfn_key]

                    fixtyperefmap = fixprop_typerefs.get(tag)
                    enum_base = {'000': '/def/enum/record/',
                                '006': '/def/enum/content/',
                                '007': '/def/enum/carrier/',
                                '008': '/def/enum/content/'}.get(tag, dfn_key)

                    if fixtyperefmap:
                        is_link = length < 3
                        if dfn_key in fixtyperefmap:
                            domainname = None
                            if tag == '000':
                                enum_base = "/def/terms/"
                            if tag == '007':
                                propname = 'carrierType'
                                repeat = True
                            elif tag in ('006', '008'):
                                propname = 'contentType'
                                repeat = True

                    overwriting = dfn_key not in tokenMaps
                    tokenmap = tokenMaps.setdefault(dfn_key, {})
                    off_key = find_boolean_off_key(tokenmap)

                    for key, dfn in valuemap.items():
                        # TODO: '_' actually means something occasionally..
                        if 'id' in dfn:
                            v = dfn['id']
                            subname = to_name(v)
                        else:
                            subname = dfn['label_sv']
                            for char, repl in [(' (', '-'), (' ', '_'), ('/', '-')]:
                                subname = subname.replace(char, repl)
                            for badchar in ',()':
                                subname = subname.replace(badchar, '')
                        if key in ('_', '|') and any(t in subname for t in ('No', 'Ej', 'Inge')):
                            continue
                        elif subname.replace('Obsolete', '') in {
                                'Unknown', 'Other', 'NotApplicable', 'Unspecified', 'Undefined'}:
                            type_id = None
                        elif key == off_key:
                            type_id = False
                        elif enum_base:
                            type_id = enum_base + subname
                        else:
                            type_id = subname
                        if overwriting:
                            assert tokenmap.get(key, type_id) == type_id, "%s: %s missing in %r" % (
                                    key, type_id, tokenmap)
                        #assert type_id is None or type_id not in enums, type_id
                        tokenmap[key] = type_id
                        #local_enums.add(type_id)
                        if include_lang:
                            dest = enum_defs[type_id] = {'@id': type_id, "@type": "Concept"}
                            add_labels(dfn, dest)

                    if skip_unlinked_maps and not is_link:
                        tokenMaps[dfn_key] = {"TODO": "SKIPPED"}

                else:
                    pass

                if domainname:
                    col_dfn['domainEntity'] = domainname

                if is_link:
                    col_dfn['addLink' if repeat else 'link'] = propname
                    col_dfn['uriTemplate'] = "{_}"
                else:
                    col_dfn['addProperty' if repeat else 'property'] = propname

                if tokenmap:
                    items = sorted((k.lower(), v) for k, v in tokenmap.items())
                    tkey = get_tokenmap_key(dfn_key, items)
                    if tkey != dfn_key:
                        del tokenMaps[dfn_key]
                    else:
                        tokenMaps[dfn_key] = OrderedDict(items)
                    col_dfn['tokenMap' if is_link else 'patternMap'] = tkey

            #enums |= local_enums

    elif not subfields:
        outf['addProperty'] = field['id']

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
            p_key = 'addProperty' if subfield.get('repeatable', False) else 'property'
            subf[p_key] = sid
            if include_lang:
                add_labels(subfield, subf)
        if len(outf.keys()) > 1 and outf == prevf:
            section[tag] = {'inherit': prevtag}
    if show_repeatable and 'repeatable' in field:
        outf['repeatable'] = field['repeatable']
    if include_lang:
        add_labels(field, outf)
    prevtag = tag


field_index = {}
for tag, field in out['bib'].items():
    if not any(k.startswith('$') for k in field):
        continue
    def hash_dict(d):
        return tuple((k, hash_dict(v) if isinstance(v, dict) else v)
                for k, v in d.items())
    fieldhash = hash_dict(field)
    if fieldhash in field_index:
        field.clear()
        field['inherits'] = field_index[fieldhash]
    else:
        field_index[fieldhash] = tag


## sanity check..
#prevranges = None
#for k, v in compositionTypeMap.items():
#    ranges = v['partRange']
#    if prevranges and ranges - prevranges:
#        print "differs", k
#    else:
#        assert any(r in contentTypeMap for r in ranges)
#        v.pop('partRange')
#    prevranges = ranges


class SetEncoder(json.JSONEncoder):
   def default(self, obj):
     return list(obj) if isinstance(obj, set) else super(SetEncoder, self).default(obj)


print json.dumps(out,
        cls=SetEncoder,
        indent=2,
        ensure_ascii=False,
        separators=(',', ': ')
        ).encode('utf-8')
