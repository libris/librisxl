#!/usr/bin/env python

import base64

def decode_base64(coded):
    return base64.b64decode(coded)

if __name__ == '__main__':
    from sys import argv
    encoded_string = argv[1]
    print "About to decode %s" % encoded_string
    print decode_base64(encoded_string)
