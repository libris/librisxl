#!/usr/bin/env python
import sys, urllib, urllib2
try:
    from com.xhaus.jyson import JysonCodec as json
except ImportError:
    # From Python
    import json 


def transform(a_json):
    sug_json = {}

    for f in a_json['fields']:
        for k, v in f.items():
            try:
                v = v.decode('utf-8')
            except:
                1
            if k == '100':
                sug_json[k] = {}
                for sf in v['subfields']:
                    for sk, sv in sf.items():
                        sug_json[k][sk] = sv
            elif k in ['400', '678', '856']:
                f_list = sug_json.get(k, [])
                d = {}
                for sf in v['subfields']:
                    for sk, sv in sf.items():
                        d[sk] = sv
                f_list.append(d)
                sug_json[k] = f_list

    if sug_json.get('100', None):

        f_100 = ' '.join(sug_json['100'].values())
        sug_json = get_records(f_100, sug_json)

        
        
        return sug_json
    else:
        return 0
 


def get_records(f_100, sug_json):
    try:
        #urlbase = "http://libris.kb.se/xsearch?query=forf:(#100#)%20spr:swe&format=json"

        url = 'http://libris.kb.se/xsearch'
        values = {'query' : 'forf:(%s) spr:swe' % f_100, 'format' : 'json'}

        data = urllib.urlencode(values)
        reply = urllib2.urlopen(url + "?" + data)
        response = reply.read()
        print "response", type(response)
        xresult = json.loads(response)['xsearch']

        sug_json['records'] = xresult['records']
        top_3 = xresult['list'][:3]
        top_titles = {}
        for p in top_3:
            top_titles[p['identifier']] = p['title']
        # TODO: Seems the top_titles contains bad encoding. Fix.
        #sug_json['top_titles'] = top_titles


    except:
        0
    return sug_json
        


_in_console = False
try:
    data = document.getDataAsString()
    #data = utf8_data.decode('utf-8')
except UnicodeDecodeError:
    print "u" 
except:
    data = sys.stdin.read()
    _in_console = True


print "console mode", _in_console

print "data", data

sug_json = transform(json.loads(data))
r = json.dumps(sug_json)

#for 700or100 in parse_bib_document():
#    old_document = whelk.get(700or100)


print "r", r

mydoc = whelk.createDocument().withIdentifier("/%s%s" % (whelk.name, document.identifier)).withData(r).withContentType("application/json")

print "Created document with ident %s" % mydoc.identifier

print "Sparar dokument i whelken daaraa"
uri = whelk.store(mydoc)
    
print "sparat %s" % uri


if _in_console:
    print result
