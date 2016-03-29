from __future__ import print_function
import json
import sys
from collections import defaultdict, OrderedDict


args = sys.argv[1:]
stats_dest = args.pop(0)
marctype = args.pop(0) if args else 'bib'


def parse_record(line, lineno=-1):
    tab1 = line.find('\t')
    try:
        data = line[tab1:line.index('\t', tab1 + 1)] if tab1 > 0 else line
        return json.loads(data)
    except:
        print("Error in:", data, "at line:", lineno)
        raise


def clear():
    print("\x1b[2J\x1b[H")


COMBO_FLOOR = 400

EXAMPLES_LIMIT = 4

class ExampleCounter(object):
    def __init__(self):
        self.counter = 0
        self.examples = []
    def count(self, example):
        if len(self.examples) < EXAMPLES_LIMIT:
            self.examples.append(example_id)
        self.counter += 1

class CounterDict(object):
    def __init__(self, floor=None):
        self.dd = defaultdict(ExampleCounter)
        self.floor = floor
    def __getitem__(self, key):
        return self.dd[key]
    def __len__(self):
        return len(self.dd)
    @property
    def __dict__(self):
        items = self.dd.items()
        if self.floor:
            items = (item for item in items if item[1].counter >= self.floor)
        return OrderedDict(sorted(items,
            key=lambda (k, v): v.counter, reverse=True))


stats = {}
biblevelmap = {}

def dump_stats():
    print("Writing stats to <%s>" % stats_dest)
    with open(stats_dest, 'w') as f:
        json.dump(stats, f, indent=2, sort_keys=False, separators=(',', ': '),
                default=lambda o: o.__dict__)

stats.update({
    'total': 0,
    'examplesLimit': EXAMPLES_LIMIT,
    'comboFloor': COMBO_FLOOR,
    'byBiblevel': biblevelmap
})


for i, line in enumerate(sys.stdin):

    if i:
        if i % 10**4 == 0:
            clear()
            print("At {:,} (combos: {:,})".format(i,
                sum(len(stat['combos']) for stat in biblevelmap.values())))
        if i % 10**6 == 0:
            dump_stats()

    record = parse_record(line, i + 1)

    stats['total'] += 1

    recid = None
    leader = record['leader']
    rectype = leader[6]
    biblevel = leader[7] if marctype in {'bib', 'auth'} else '#'
    rectypebiblevel = rectype + biblevel

    stat = biblevelmap.get(biblevel)
    if not stat:
        stat = biblevelmap[biblevel] = {
            'count': 0,
            'combos': CounterDict(COMBO_FLOOR),
            'rectypebiblevel': defaultdict(int)
        }

    stat['count'] += 1
    stat['rectypebiblevel'][rectypebiblevel] += 1

    tag_count = defaultdict(int)
    for field in record['fields']:

        assert len(field) == 1
        tag, value = field.items()[0]
        tag_count[tag] += 1

        fieldstats = stat.get(tag)
        if not fieldstats:
            subcombos = CounterDict()
            fieldstats = stat[tag] = {
                'count': 0,
                'repeats': CounterDict(),
                'combos': subcombos
            }
        else:
            subcombos = fieldstats['combos']
        fieldstats['count'] += 1

        codes = []
        if tag == '001':
            example_id = "%s:%s (%s)" % (marctype, value, rectypebiblevel)
        elif tag in {'003', '005', '006', '007', '008'}:
            pass
        else:
            if isinstance(value, unicode):
                pass # report error?
            elif isinstance(value, dict):
                for n in ('1', '2'):
                    indval = value['ind%s' % n].strip()
                    if indval:
                        ind = 'i%s=%s' % (n, indval)
                        codes.append(ind)
                        fieldstats[ind] = fieldstats.get(ind, 0) + 1
                for sub in value['subfields']:
                    for code, subvalue in sub.items():
                        codes.append(code)
                        fieldstats[code] = fieldstats.get(code, 0) + 1

        stat[tag]['repeats']['x%s' % tag_count[tag]].count(example_id)

        code_combo = " ".join(codes)
        subcombos[code_combo].count(example_id)

    combos = sorted(set(
        tag if tag[0] == '2' or tag[0:2] == '00' else
        '01x-04x' if tag[0] == '0' and tag[1] in '1234' else
        '05x-08x' if tag[0] == '0' and tag[1] in '5678' else
        '%sxx' % tag[0]
        for tag in tag_count))
    combo_key = " ".join(combos)

    stat['combos'][rectypebiblevel +' '+ combo_key].count(example_id)

dump_stats()
