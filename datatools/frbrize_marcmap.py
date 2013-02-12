import re
from collections import namedtuple, OrderedDict
from itertools import starmap
import csv

from sys import stderr
import cgitb; cgitb.enable(format='text')
def error(msg):
    print >>stderr, msg


class Item(namedtuple('Item',
        "f1 f2 rectype field fieldname subfield position name comment entity"
        " description f5 f6 f7 f8 f9 f10 f11 f13 f14 f15 f16 f17 f18 f19")):
    pass


def get_items(fpath):
    with open(fpath, 'rb') as f:
        reader = csv.reader(f)
        for item in starmap(Item, reader):
            field = item.field
            fixmap = None
            if field == 'ldr':
                field = '000'
            elif field == 'dir':
                continue # TODO: ok? Directory is about low-level syntax parsning
            elif len(field) > 3 and field[3].isalpha():
                field, rectype = field[0:3], field[3:]
                if field == '006':
                    renames = {'Continuing': 'Serial'}
                elif field == '007':
                    renames = {
                        'Electronic': 'Computer',
                        'NPG': 'NonprojectedGraphic',
                        'MP': 'MotionPicture',
                        'Music': 'NotatedMusic',
                        'RSI': 'RemoteSensingImage',
                        'SR': 'SoundRecording',
                    }
                elif field == '008':
                    renames = {'All': ''}
                else:
                    renames = {}
                rectype = renames.get(rectype, rectype)
                fixmap = field + "_" + rectype
            else:
                fixmap = None
            item = item._replace(
                    field=field,
                    rectype=item.rectype.split('/'),
                    position=item.position if item.position != 'n/a' else None,
                    subfield=item.subfield if item.subfield != 'n/a' else None,
                    entity=re.sub(r'\?|\+[EA]\d+|^n/a$', '',
                        item.entity.replace('\x98', '')).title().replace(' ', ''))
            item.fixmap = fixmap
            yield item

def dump_entities(items):
    entities = set(item.entity for item in items)
    for it in entities:
        if it:
            print it

def add_entities_to_marcmap(marcmap, items):
    for item in items:
        if not item.entity:
            continue
        if "BD" in item.rectype:
            recmap = marcmap['bib']
        #elif "HD" in item.rectype
        #    recmap = marcmap['holdings']
        else:
            continue
        field = recmap.get(item.field)
        if not field:
            error("Unknown field:", item.field)
            continue
        if item.subfield:
            try:
                field['subfield'][item.subfield]['entity'] = item.entity
            except KeyError:
                error("Unknown field: {0.field}, subfield: {0.subfield}".format(item))
        elif item.position:
            matchmap = None
            # TODO: just guessing about 01 and 02
            if item.position == '01':
                field['ind1_entity'] = item.entity
            elif item.position == '02':
                field['ind2_entity'] = item.entity
            elif item.fixmap:
                matchmap = item.fixmap.split('/')[0]
            elif item.field == '000':
                matchmap = '000_BibLeader'

            if matchmap and 'fixmaps' in field:

                for fixmap in field['fixmaps']:
                    name = fixmap['name']
                    if name.endswith('s') and not matchmap.endswith('s'):
                        name = name[0:-1]
                    if name.startswith(matchmap):
                        break
                else:
                    error("Found no fixmap for field {0} matching {1} (entity: {2})".format(
                            item.field, matchmap, item.entity))
                if '-' in item.position:
                    item_start, item_stop = map(int, item.position.split('-'))
                    # TODO: is this always information about repeated attributes?
                else:
                    item_start, item_stop = [int(item.position)] * 2
                columns = fixmap['columns']
                for col in columns:
                    offset, length = col['offset'], col['length']
                    start, stop = offset, offset + length - 1
                    if  start >= item_start and stop <= item_stop:
                        col['entity'] = item.entity
            else:
                field['entity'] = item.entity
        else:
            field['entity'] = item.entity


if __name__ == '__main__':
    from sys import argv, stdout
    frbrcsv_path = argv[1]
    marcmap_path = argv[2] if len(argv) > 2 else None

    # Use CSV from:
    #   <http://www.loc.gov/marc/marc-functional-analysis/source/FRBR_Web_Copy.txt>
    items = get_items(frbrcsv_path)

    if marcmap_path == '-e':
        dump_entities(items)
    elif not marcmap_path:
        for item in items:
            print item.field, item.subfield or item.position, item.entity
    else:
        import json
        with open(marcmap_path) as f:
            marcmap = json.load(f, object_pairs_hook=OrderedDict)
        add_entities_to_marcmap(marcmap, items)
        json.dump(marcmap, stdout, indent=2)

