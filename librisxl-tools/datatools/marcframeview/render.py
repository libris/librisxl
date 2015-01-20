#!/usr/bin/env python
import json
from jinja2 import Environment, PackageLoader


env = Environment(loader=PackageLoader(__name__, '.'),
        variable_start_string='${', variable_end_string='}',
        line_statement_prefix='%')
tplt = env.get_template('template.html')


def render(marcframe):

    MARC_CATEGORIES = 'bib', 'auth', 'hold'

    def marc_categories():
        for cat in MARC_CATEGORIES:
            yield cat, marcframe[cat]

    def fields(catdfn):
        for tag, dfn in sorted(catdfn.items()):
            if tag.isdigit() and dfn:
                kind = ('fixed' if any(k for k in dfn if k[0] == '[' and ':' in k)
                        else 'field' if any(k for k in dfn if k[0] == '$')
                        else 'control')
                yield tag, kind, dfn

    def codes(dfn):
        for code, subdfn in sorted(dfn.items()):
            if code.startswith('$') and subdfn:
                yield code, subdfn

    return tplt.render(dict(vars(__builtins__), **vars())).encode('utf-8')


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    marcframe_path = args.pop(0)
    with open(marcframe_path) as fp:
        marcframe = json.load(fp)
    sys.stdout.write(render(marcframe))
