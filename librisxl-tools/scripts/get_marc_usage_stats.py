from __future__ import unicode_literals, print_function
__metaclass__ = type

import json
import sys
from collections import defaultdict, OrderedDict


COLSPECS = {}
COLSPECS['bib'] = {
    '000': [
        [5], # marc:status
        [6], # @type
        [7], # @type
        [8], # marc:typeOfControl
        [9], # marc:characterCoding
        [17], # marc:encLevel
        [18], # marc:catForm
        [19], # marc:linked
    ],
    '006': [
        [0], # hasPart / @type
    ],
    '007': [
        [0], # hasFormat / @type
    ],
    '008': [
        [6], # marc:publicationStatus
        [38], # marc:modifiedRecord
        [39], # marc:catalogingSource
    ]
}
COLSPECS['auth'] = {
    '000': COLSPECS['bib']['000'],
    '008': [
        [6], # marc:subdivision
        [7], # marc:romanization
        [8], # marc:languageOfCatalog
        [9], # marc:kindOfRecord
        [10], # marc:catalogingRules
        [11], # marc:subjectHeading
        [12], # marc:typeOfSeries
        [13], # marc:numberedSeries
        [14], # marc:headingMain
        [15], # marc:headingSubject
        [16], # marc:headingSeries
        [17], # marc:subjectSubdivision
        [28], # marc:govtAgency
        [29], # marc:reference
        [31], # marc:recordUpdate
        [32], # marc:personalName
        [33], # marc:level
        [38], # marc:modifiedRecord
        [39], # marc:catalogingSource
    ],
}
COLSPECS['hold'] = {
    '000': [
        [5], # marc:status
        [6], # @type
        [7], # marc:statistics
        [9], # marc:characterCoding
        [17], # marc:encLevel
        [18], # marc:itemInfoInRecord
    ],
    '007': COLSPECS['bib']['007'],
    '008': [
        [6], # marc:acquisitionStatus
        [7], # marc:acquisitionMethod
        [12], # marc:retentionPolicy
        [13], # marc:specificRetentionPolicy
        [14], # marc:retentionPolicyNumberOfUnit
        [15], # marc:retentionPolicyUnitType
        [16], # marc:completeness
        [17], # marc:numberOfItems
        [20], # marc:lendingPolicy
        [21], # marc:reproductionPolicy
        #[22,25], # language
        [25], # marc:copyReport
    ]
}

COMBO_FLOOR = 400

EXAMPLES_LIMIT = 16


class Stats:

    def __init__(self, marctype, dest_file):
        self.dest_file = dest_file
        self.marctype = marctype
        self.biblevelmap = {}
        self.stats = {
            'total': 0,
            'examplesLimit': EXAMPLES_LIMIT,
            'comboFloor': COMBO_FLOOR,
            'byBiblevel': self.biblevelmap
        }

    def dump(self):
        print("Writing stats to <%s>" % self.dest_file)
        with open(self.dest_file, 'w') as f:
            json.dump(self.stats, f, indent=2, sort_keys=False, separators=(',', ': '),
                    default=lambda o: o.__dict__)

    def process_record(self, record):
        self.stats['total'] += 1

        recid = None
        leader = record['leader']

        rectype = leader[6]

        # really only in bib, but measured for errors in auth as well
        # (in hold, this position is for statistics data)
        biblevel = leader[7] if self.marctype in {'bib', 'auth'} else '#'

        rectypebiblevel = rectype + biblevel

        recstats = self.biblevelmap.get(biblevel)
        if not recstats:
            recstats = self.biblevelmap[biblevel] = {
                'count': 0,
                'combos': CounterDict(COMBO_FLOOR),
                'rectypebiblevel': defaultdict(int)
            }

        recstats['count'] += 1
        recstats['rectypebiblevel'][rectypebiblevel] += 1

        tag_count = defaultdict(int)
        record_id = None
        for field in record['fields']:
            assert len(field) == 1
            tag, value = field.items()[0]
            tag_count[tag] += 1

            if tag == '001':
                assert record_id is None
                record_id = "%s:%s (%s)" % (self.marctype, value, rectypebiblevel)

            self.process_field(recstats, tag, value, record_id)

            recstats[tag]['repeats']['x%s' % tag_count[tag]].count(record_id)

        combos = sorted(set(
            tag if tag[0] == '2' or tag[0:2] == '00' else
            '01x-04x' if tag[0] == '0' and tag[1] in '1234' else
            '05x-08x' if tag[0] == '0' and tag[1] in '5678' else
            '%sxx' % tag[0]
            for tag in tag_count))
        combo_key = " ".join(combos)

        recstats['combos'][rectypebiblevel +' '+ combo_key].count(record_id)

        self.process_field(recstats, '000', leader, record_id)

    def process_field(self, recstats, tag, value, record_id):
        fieldstats = recstats.get(tag)
        if not fieldstats:
            subcombos = CounterDict()
            fieldstats = recstats[tag] = {
                'count': 0,
                'repeats': CounterDict(),
                'combos': subcombos,
                'errors': CounterDict()
            }
        else:
            subcombos = fieldstats['combos']
        fieldstats['count'] += 1

        if tag in {'000', '001', '003', '005', '006', '007', '008'}:
            if isinstance(value, unicode):
                if tag in {'000', '006', '007', '008'}:
                    self.measure_fixed(fieldstats, tag, value, record_id)
            else:
                fieldstats['errors']['expected_fixed'].count(record_id)
        else:
            if isinstance(value, unicode):
                fieldstats['errors']['expected_subfields_got_fixed'].count(record_id)
            elif isinstance(value, dict):
                codes = self.measure_subfields(fieldstats, tag, value)
                code_combo = " ".join(codes)
                subcombos[code_combo].count(record_id)

    def measure_fixed(self, fieldstats, tag, value, record_id):
        colspecs = COLSPECS.get(self.marctype, {}).get(tag, ())
        for col in colspecs:
            slicecode = '[%s]' % ':'.join(map(str, col))
            colslice = col[0] if len(col) == 1 else slice(*col)
            try:
                key = '%s=%s' % (slicecode, value[colslice])
            except IndexError:
                fieldstats['errors']['failed_slice-%s' % slicecode].count(record_id)
            else:
                fieldstats[key] = fieldstats.get(key, 0) + 1

    def measure_subfields(self, fieldstats, tag, value):
        codes = []
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
        return codes


class ExampleCounter(object):
    """
    Usage:
    >>> counter = ExampleCounter(limit=4)
    >>> for i in range(8):
    ...     counter.count(str(i + 1))
    ...     print(', '.join(counter.examples))
    1
    1, 2
    1, 2, 3
    1, 2, 3, 4
    2, 3, 4, 5
    3, 4, 5, 6
    4, 5, 6, 7
    5, 6, 7, 8
    """
    def __init__(self, limit=EXAMPLES_LIMIT):
        self.limit = limit
        self.total = 0
        self.examples = []
    def count(self, example):
        if len(self.examples) == self.limit:
            self.examples.pop(0)
        self.examples.append(example)
        self.total += 1


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
            items = (item for item in items if item[1].total >= self.floor)
        return OrderedDict(sorted(items,
            key=lambda (k, v): v.total, reverse=True))


def compute_stats(marctype, stats_dest):
    stats = Stats(marctype, stats_dest)

    for i, line in enumerate(sys.stdin):
        if i:
            if i % 10**4 == 0:
                clear()
                print("At {:,} (combos: {:,})".format(i,
                    sum(len(stat['combos']) for stat in stats.biblevelmap.values())))
            if i % 10**6 == 0:
                stats.dump()

        record = parse_record(line, i + 1)
        stats.process_record(record)

    stats.dump()


def parse_record(line, lineno=-1):
    tab1 = line.find(b'\t')
    if tab1 > 0: # expect PSQL TSV import format
        data = line[tab1:line.index(b'\t', tab1 + 1)].replace(b'\\\\', b'\\')
    else: # one JSON document per line
        data = line
    try:
        return json.loads(data)
    except:
        print("Error in:", data, "at line:", lineno)
        raise


def clear():
    print("\x1b[2J\x1b[H")


if __name__ == '__main__':
    args = sys.argv[1:]

    if not args:
        import doctest
        doctest.testmod()
        exit()

    marctype = args.pop(0) if args else 'bib'
    stats_dest = args.pop(0)

    compute_stats(marctype, stats_dest)
