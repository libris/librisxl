#!/usr/bin/env python
from __future__ import unicode_literals, print_function
import re
from os import makedirs, path as P

find_token = re.compile(r'{"(100|110|111|130|148|150|151|155|162|180|181|182|185)":').findall

def split_auth_source(sourcefile, outdir):
    name_parts = P.basename(sourcefile).split('.', 1)
    if not P.exists(outdir):
        makedirs(outdir)
    outfiles = {}
    try:
        source = open(sourcefile)
        for i, l in enumerate(source):
            for token in find_token(l):
                outfp = outfiles.get(token)
                if not outfp:
                    outfile = P.join(outdir,
                            '%s-%s.%s' % (name_parts[0], token, name_parts[1]))
                    print("Opening %s for writing..." % outfile)
                    outfp = outfiles[token] = open(outfile, 'w')
                print(l, end="", file=outfp)
                break
    finally:
        source.close()
        for outfp in outfiles.values():
            outfp.close()

if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    sourcefile = args.pop(0)
    outdir = args.pop(0)
    split_auth_source(sourcefile, outdir)
