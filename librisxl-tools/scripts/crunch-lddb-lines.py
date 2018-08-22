from __future__ import print_function
import json
import sys
import re

def parse_select(sel_str):
    steps = []
    match_rule = None
    for word in sel_str.strip().split(' '):
        if match_rule:
            if match_rule == '=':
                steps.append(lambda data, word=word: data == word)
            elif match_rule == '=~':
                matcher = re.compile(word)
                steps.append(lambda data: isinstance(data, unicode) and matcher.match(data))
            match_rule = None
            continue

        match_rule = None
        if word == '{':
            parent_steps, test_steps = steps, []
            steps = test_steps
        elif word == '}':
            parent_steps.append(lambda data, steps=test_steps: match_selector(steps, data))
            steps = parent_steps
            parent_steps, test_steps = None, None
        elif word in {'=', '=~'}:
            match_rule = word
        elif word.isdigit():
            steps.append(int(word))
        else:
            steps.append(word)

    return steps

def match_selector(selector, data):
    current = data
    for i, step in enumerate(selector):
        if callable(step):
            if step(current):
                continue
            else:
                return False

        if isinstance(step, int):
            current = current[step]
        else:
            if isinstance(current, list):
                selector_trail = selector[i:]
                for item in current:
                    if match_selector(selector_trail, item):
                        return item

                return False
            else:
                current = current.get(step)
        if current is None:
            return False

    return current

if __name__ == '__main__':
    args = sys.argv[1:]

    selector = parse_select(args.pop(0)) if args else None
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
                result = match_selector(selector, data)
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
