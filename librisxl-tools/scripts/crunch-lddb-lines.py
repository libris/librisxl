"""
This began as just an example script for running against a stream of JSON lines
(specifically a PGSQL data dump with double escapes.) Then the Selector utility grew out ouf it...

A*void developing this further*. Mostly, you will get by by using something
like JQ (https://stedolan.github.io/jq/). Or by basing your own analysis on a
pattern like the main loop of this script.

Still, for reference, invoke this like:

    $ SOME_PSQL_JSON_STREAM | python crunch-lddb-lines.py '@graph { inScheme @id =~ .*/sao$ } @type = Topic'

"""
from __future__ import print_function
import json
import sys
import re


class Selector(object):

    def __init__(self, sel_str):
        steps = []
        match_rule = None
        for word in sel_str.strip().split(' '):
            if match_rule:
                if match_rule == '=':
                    steps.append(lambda data, word=word: data == word)
                elif match_rule == '=~':
                    matcher = re.compile(word)
                    steps.append(lambda data:
                            isinstance(data, unicode) and matcher.match(data))
                match_rule = None
                continue

            match_rule = None
            if word == '{':
                parent_steps, test_steps = steps, []
                steps = test_steps
            elif word == '}':
                parent_steps.append(lambda data, steps=test_steps:
                        self.match_selector(steps, data))
                steps = parent_steps
                parent_steps, test_steps = None, None
            elif word in {'=', '=~'}:
                match_rule = word
            elif word.isdigit():
                steps.append(int(word))
            else:
                steps.append(word)

        self.steps = steps

    def __call__(self, data):
        return self.match_selector(self.steps, data)

    @classmethod
    def match_selector(cls, steps, data):
        current = data
        for i, step in enumerate(steps):
            if callable(step):
                if step(current):
                    continue
                else:
                    return False

            if isinstance(step, int):
                current = current[step]
            else:
                if isinstance(current, list):
                    steps_trail = steps[i:]
                    for item in current:
                        if cls.match_selector(steps_trail, item):
                            return item

                    return False
                else:
                    current = current.get(step)
            if current is None:
                return False

        return current


if __name__ == '__main__':
    args = sys.argv[1:]

    selector = Selector(args.pop(0)) if args else None
    match_count = 0

    for i, l in enumerate(sys.stdin):
        if not l.rstrip():
            continue
        l = l.replace(b'\\\\"', b'\\"')
        if i % 100000 == 0:
            print("At line", i, file=sys.stderr)
        try:
            data = json.loads(l)
            try:
                data_id = data.get('@id') or data['@graph'][0]['@id']
            except KeyError:
                data_id = None
            if selector:
                result = selector(data)
                if result:
                    match_count += 1
                    print("Line", i, "id", data_id, "matched on", json.dumps(result))
                    sys.stdout.flush()
        except ValueError as e:
            print("ERROR at", i, "in data:", file=sys.stderr)
            print(l, file=sys.stderr)
            print(e, file=sys.stderr)
            for char_index in re.findall(r'.+\(char (\d+)\)$', e.message):
                char_index = int(char_index)
                print(l[char_index - 20 if char_index > 20 else char_index : char_index + 20], file=sys.stderr)
                print(('-' * 20) + '^', file=sys.stderr)
                break

    if selector:
        print("Total matches:", match_count)
