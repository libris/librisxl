#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from os.path import basename, join as pjoin
from ConfigParser import RawConfigParser
from collections import OrderedDict as odict
import json

from marcmap_bandaid import propRefMapper as bandaid

ENC = "latin-1"

def parse_configs(confdir, lang):
    # FIXME: Errors in english version:
    # - Bmarcfix.cfg:267 has a leading space which ConfigParser fails on
    # - Master.cfg:134 repeats position key 13 twice (change second to e.g. 1301 to make it work)
    # TODO: add ['obsolete'] = True if "OBSOLETE" in label

    master_cfg = read_config(pjoin(confdir, "master.cfg"))

    afix_cfg = read_config(pjoin(confdir, "Amarcfix.cfg"))
    AUTH_CONFS = ["Amarc{0}xx.cfg".format(i) for i in xrange(9)]
    a_cfg = read_config(pjoin(confdir, f) for f in AUTH_CONFS)

    bfix_cfg = read_config(pjoin(confdir, "Bmarcfix.cfg"))
    BIB_CONFS = ["Bmarc{0}xx.cfg".format(i) for i in xrange(10)]
    b_cfg = read_config(pjoin(confdir, f) for f in BIB_CONFS)

    #HOLDINGS_CONFS = ["Hmarc{0}xx.cfg".format(i) for i in (0, 3, 4, 6, 7, 8, 9)]
    #hfix_cfg = read_config(pjoin(confdir, "Hmarcfix.cfg"))
    #"country.cfg"
    #"lang.cfg"

    out = odict()


    categories = [('bib', "Bibliographic", b_cfg),
                ('auth', "Authority", a_cfg),]
                #('hold', "Holdings", h_cfg)]

    for cat_id, name, cfg in categories:
        block = out[cat_id] = odict()
        section = name + " Fields"
        mandatory = set(v.split()[0]
                for k, v in master_cfg.items("Mandatory " + section))
        for key, value in master_cfg.items(section):
            # Works only because these come after the field numbers..
            if key.startswith("Field"):
                tagref = key[5:]
                field, label = block[tagref], value.decode(ENC)
                if lang == 'en':
                    field['id'] = label_to_id(label)
                field['label_'+lang] = label
            elif key.strip().isdigit():
                tag, repeatable = value.rstrip().split()
                subfields = odict()
                inds = [None, None]
                if cfg.has_section(tag):
                    for subkey, subval in cfg.items(tag):
                        # Works only because [...] after [...] ..
                        if subkey.startswith("Subf"):
                            code = subkey[4:]
                            if code in subfields:
                                subfield, label = subfields[code], subval.decode(ENC)
                                if lang == 'en':
                                    subfield['id'] = label_to_id(label)
                                subfield['label_'+lang] = label
                        elif subkey.isdigit():
                            code, subrepeat_repr = subval.rstrip().split(None, 1)
                            sub_repeatable = subrepeat_repr[0] == '1'
                            sub_mandatory = subrepeat_repr[-1] == 'M'
                            sub_applicable = subrepeat_repr[-1] == 'A'
                            subfields[code] = subfield = odict(id=None)
                            subfield['label_'+lang] = None
                            subfield['repeatable'] = sub_repeatable
                            if sub_mandatory:
                                subfield['mandatory'] = sub_mandatory
                            #if sub_applicable:
                            #    subfield['applicable'] = sub_applicable
                    for i in (1, 2):
                        indKey = '{0}Ind{1}'.format(tag, i)
                        if cfg.has_section(indKey):
                            ind = inds[i-1] = odict()
                            for indcode, indval in cfg.items(indKey):
                                if indcode.startswith('Value'):
                                    ind[indcode[5:]] = labelled(indval.decode(ENC), lang)
                                    # TODO: if '_' and id == 'undefined': None?
                dfn = odict(id=None)
                dfn['label_'+lang] = None
                dfn['repeatable'] = bool(int(repeatable))
                if tag in mandatory or tag[0] + 'XX' in mandatory:
                    dfn['mandatory'] = True
                for i, ind in enumerate(inds):
                    if ind:
                        dfn['ind' + str(i+1)] = ind
                if subfields:
                    dfn['subfield'] = subfields
                block[tag] = dfn
            else:
                raise ValueError("Unknown block key: %s" % key)


    rec_term_map = odict()
    for combo, term in master_cfg.items("RecFormat"):
        rec_term_map.setdefault(term, set()).add(combo)
    #assert set(rec_term_map) == set(['Authority', 'Holding']
    #        + ['Book', 'Computer', 'Map', 'Mixed', 'Music', 'Serial', 'Visual'])


    for block_key, fix_tags, fix_cfg in [
            ('bib', ['000', '006', '007', '008'], bfix_cfg),
            ('auth', ['000', '008'], afix_cfg)]:
        block = out[block_key]
        fixprops = block['fixprops'] = odict()
        _fixprop_unique = {}

        for tagcode in fix_tags:
            fixmaps = odict()
            for key, value in fix_cfg.items(tagcode + 'Code'):
                tablelabel, tablename = value.split(',')
                tablename = tablename.strip()
                table = fixmaps.setdefault(tablename, odict())
                table['name'] = tablename
                table.setdefault('matchKeys', []).append(key)
                if tablelabel in rec_term_map:
                    table['term'] = tablelabel
                    table['matchRecTypeBibLevel'] = list(rec_term_map[tablelabel])
                else:
                    table['label_'+lang] = tablelabel.decode(ENC)
                columns = table['columns'] = []
                for tablerow, tableval in fix_cfg.items(tablename):
                    if tableval:
                        tablerow = tablerow + tableval
                    cells = [s.strip() for s in tablerow.decode(ENC).split(',')]
                    label, enumkey, offset, length, default = cells
                    col = odict()
                    col['label_'+lang] = label
                    col['offset'] = int(offset)
                    col['length'] = int(length)
                    col['default'] = default
                    prop_id = fixkey_to_prop_id(enumkey)
                    if fix_cfg.has_section(enumkey):
                        # ensure converted name is still unique
                        if prop_id in _fixprop_unique:
                            assert _fixprop_unique[prop_id] == enumkey
                        else:
                            _fixprop_unique[prop_id] = enumkey
                        col['propRef'] = prop_id
                        if prop_id not in fixprops:
                            fixprops[prop_id] = fixprop = odict()
                            for k, v in fix_cfg.items(enumkey):
                                fixprop[k] = labelled(v.decode(ENC), lang)
                    else:
                        # should we really keep placeholder here?
                        col['placeholder'] = prop_id
                        try:
                            col['propRef'] = bandaid[col['label_' + lang]]
                        except: 
                            1
                    columns.append(col)

            block[tagcode]['fixmaps'] = fixmaps.values()

    return out


def read_config(paths):
    cfg = RawConfigParser(allow_no_value=True)
    cfg.optionxform = str
    cfg.read(paths)
    return cfg

def labelled(label, lang):
    obj = odict()
    if lang == 'en':
        obj['id'] = label_to_id(label)
    obj['label_'+lang] = label
    return obj

def label_to_id(label):
    if not label:
        return None
    key = label.title()
    key = re.sub(r"\W", "", key)
    return key[0].lower() + key[1:]

def fixkey_to_prop_id(key):
    ref = re.sub(r"^\d+(/\d+)?", "", key)
    return ref[0].lower() + ref[1:]


def columnsdict(l):
    d = odict()
    for o in l:
        if 'offset' in o and 'length' in o:
            d['{offset}:{length}'.format(**o)] = o
        else:
            break
    return d

def dmerge(a, b):
    for k, v in b.items():
        if isinstance(v, dict) and k in a:
            dmerge(a[k], v)
        elif isinstance(v, list) and k in a:
            adict = columnsdict(a[k])
            bdict = columnsdict(v)
            if adict or bdict:
                a[k] = dmerge(adict, bdict).values()
            else:
                a[k] = [dmerge(alv, blv) if isinstance(alv, dict) else alv
                        for alv, blv in zip(a[k], v)]
        elif k not in a or not a[k]:
            a[k] = v
    return a


if __name__ == '__main__':

    from sys import argv, stdout
    from itertools import izip_longest
    def grouped(n, iterable): return izip_longest(*[iter(iterable)] * n)

    cmd, args = basename(argv[0]), argv[1:]
    if len(args) < 2 or len(args) % 2 != 0:
        print "Usage: {0} CONFIG_DIR LANG [[CONFIG_DIR LANG]...]".format(cmd)
        exit()

    prev = None
    for confdir, lang in grouped(2, args):
        out = parse_configs(confdir, lang)
        prev = dmerge(out, prev) if prev else out

    json.dump(out, stdout, indent=2)

