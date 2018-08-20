from __future__ import unicode_literals, print_function
import sys
import os
import re


outfile = None

def next_outfile(basepath, i):
    global outfile
    fpath = "{}-{}.jsonld".format(basepath, i)
    dirname = os.path.dirname(fpath)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    outfile = open(fpath, 'w')

def process_input(line_stream, basepath, chunksize, context_path):
    try:
        for i, line in enumerate(line_stream):
            line = line.strip()
            if not line:
                continue

            if i % chunksize == 0:
                if outfile:
                    print(b']}', file=outfile)
                next_outfile(basepath, i)
                print(b'{"@graph": [', file=outfile)
            else:
                print(', ', end="", file=outfile)

            process_record_line(i, line, outfile, context_path)

        print(b']}', file=outfile)
    finally:
        if outfile:
            outfile.close()

def process_record_line(i, line, outfile, context_path):
    # find record id
    for rec_id in re.findall(r'{"@graph": \[{"@id": "([^"]+)', line):
        break
    else:
        print("Unable to find an IRI in line {0}:".format(i),
                file=sys.stderr)
        print(line, file=sys.stderr)
        return

    # add record id to top graph to name it
    line = b'{{"@id": "{0}", {1}'.format(rec_id, line[1:])

    # add context reference
    line = b'{{"@context": "{0}", {1}'.format(context_path, line[1:])

    # * Fix broken @id values:
    # Remove Unicode control characters (mostly harmful in terms and ids)
    line = re.sub(r'[\x00-\x1F\x7F]', b'', line)
    line = re.sub(r'\\{2}', br'\\', line)
    # TODO: @id values, replace(' ', '+') and replace('\\', r'\\')

    # Add "marker" for blank nodes to cope with BlazeGraph limitation
    line = re.sub(r'{}', b'{"@id": "owl:Nothing"}', line)
    # Or remove empty blank nodes entirely?
    #line = re.sub(r',{}|{},?', '', line)

    print(line, file=outfile)


if __name__ == '__main__':
    args = sys.argv[1:]

    basepath = args.pop(0) if args else 'data'
    chunksize = int(args.pop(0)) if args else 100 * 1000
    context_path = args.pop(0) if args else 'context.jsonld'

    process_input(sys.stdin, basepath, chunksize, context_path)
