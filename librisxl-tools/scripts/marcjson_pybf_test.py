# -*- coding: UTF-8 -*-
import json
from bibframe.reader import marcxml
from bibframe.reader import bfconvert


def parse_record(record, handler):
    handler.start_element('record', {})

    handler.start_element('leader', {})
    handler.char_data(record['leader'])
    handler.end_element('leader')

    for field in record['fields']:
        for tag, value in field.items():
            attrs = {'tag': tag}
            if isinstance(value, dict):
                for ind in ["ind1", "ind2"]:
                    attrs[ind] = value.get(ind, " ")
                handler.start_element('datafield', attrs)
                for subfield in value['subfields']:
                    for code, subval in subfield.items():
                        handler.start_element('subfield', {'code': code})
                        handler.char_data(subval)
                        handler.end_element('subfield')
                handler.end_element('datafield')
            else: # fixed
                handler.start_element('controlfield', attrs)
                handler.char_data(value)
                handler.end_element('controlfield')

    handler.end_element('record')


class FauxParser:
    CurrentLineNumber = -1
    CurrentColumnNumber = -1

def handle_marcjson_source(infname, sink, args, attr_cls, attr_list_cls):
    parser = FauxParser()
    with open(infname) as fp:
        record = json.load(fp)
    next(sink)
    handler = marcxml.expat_callbacks(sink, parser, attr_cls, attr_list_cls, lax=True)
    result = parse_record(record, handler)


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    infile = args.pop(0)
    outfile = args.pop(0)
    with open(outfile, 'wb') as fp:
        bfconvert([infile], handle_marc_source=handle_marcjson_source,
                rdfttl=fp, verbose=True)
