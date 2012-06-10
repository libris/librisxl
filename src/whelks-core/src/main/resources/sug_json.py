#!/usr/bin/env python
import sys, urllib, urllib2
try:
    from com.xhaus.jyson import JysonCodec as json
except ImportError:
    # From Python
    import json 


def transform(a_json, rtype):
    alla_json = []
    resten_json = {}
    link = None
    id001 = None

    top_title = None

    for f in a_json['fields']:
        for k, v in f.items():
            if k == '001':
                link = "/%s/%s" % (rtype, v)
                id001 = v
            if k in ['100', '700']:
                should_add = True
                sug_json = {}
                sug_json['100'] = {}
                sug_json['100']['ind1'] = v['ind1']
                for sf in v['subfields']:
                    for sk, sv in sf.items():
                        if sk == '0' and rtype == 'bib':
                            should_add = False
                        if sk in ['a','b','c','d']:
                            sug_json['100'][sk] = sv
                if should_add:
                    if k == '100':
                        sug_json['identifier'] = "/%s/%s/%s" % (w_name, suggest_source, id001)
                    else:
                        name = "%s_%s" % (id001, '_'.join(sug_json['100'].values()[1:]).replace(",","").replace(" ", "_").replace(".","").replace("[","").replace("]",""))

                        print "values", w_name, suggest_source, "name:", name
                        sug_json['identifier'] = "/%s/%s/%s" % (w_name, suggest_source, name)
                    alla_json.append(sug_json)
            elif k in get_fields(rtype):
                f_list = resten_json.get(k, [])
                d = {}
                for sf in v['subfields']:
                    for sk, sv in sf.items():
                        d[sk] = sv
                f_list.append(d)
                if k == '245':
                    titleparts = {}
                    for sf in v['subfields']:
                        for sk, sv in sf.items():
                            if sk in ['a', 'b']:
                                titleparts[sk] = sv.strip('/ ')
                                
                    print "titleparts", titleparts
                    top_title = "%s %s" % (titleparts['a'], titleparts.get('b', ''))
                    
                resten_json[k] = f_list

    # get_records for auth-records
    if rtype == 'auth':
        if alla_json[0].get('100', None):
            f_100 = ' '.join(alla_json[0]['100'].values()[1:])
            resten_json = get_records(f_100, resten_json)
    # single top-title for bibrecords
    elif top_title: 
        resten_json['top_titles'] = {"%s%s" % ("http://libris.kb.se", link) : top_title.strip()}

    # More for resten
    if link:
        resten_json['link'] = link
    resten_json['authorized'] = True if rtype == 'auth' else False
        
    # cleanup
    try:
        resten_json.pop('245')
    except:
        1

    # append resten on alla
    print "resten", resten_json
    ny_alla = []
    for my_json in alla_json:
        ny_alla.append(dict(my_json.items() + resten_json.items()))

    print "alla", ny_alla
    return ny_alla
 
def get_fields(rtype):
    if rtype == 'auth':
        return ['400','500','678','680','856']
    return ['245', '400','678']


def get_records(f_100, sug_json):
    try:
        url = 'http://libris.kb.se/xsearch'
        values = {'query' : 'forf:(%s) spr:swe' % f_100, 'format' : 'json'}

        data = urllib.urlencode(values)
        reply = urllib2.urlopen(url + "?" + data)

        response = reply.read().decode('utf-8')
        print "got response", response, type(response)

        xresult = json.loads(response)['xsearch']

        sug_json['records'] = xresult['records']
        top_3 = xresult['list'][:3]
        top_titles = {}
        for p in top_3:
            top_titles[p['identifier']] = unicode(p['title'])
        sug_json['top_titles'] = top_titles

    except:
        print "exception"
        0
    return sug_json

def record_type(leader):
    if list(leader)[6] == 'z':
        return "auth"
    return "bib"


_in_console = False
try:
    data = document.getDataAsString()
    ctype = document.getContentType()
except:
    ctype = "application/json"
    data = sys.stdin.read()
    whelk = None
    _in_console = True



#print "console mode", _in_console

if ctype == 'application/json':
    in_json = json.loads(data)
    rtype = record_type(in_json['leader'])
    suggest_source = rtype 
    if (rtype == 'bib'):
        suggest_source = 'name'

    w_name = whelk.name if whelk else "test"

    #identifier = "/%s/%s/%s" % (w_name, suggest_source, document.identifier.toString().split("/")[-1])
    sug_jsons = transform(in_json, rtype)
    for sug_json in sug_jsons:
        identifier = sug_json['identifier'] 
        r = json.dumps(sug_json)
        print "r", r

        if whelk:
            mydoc = whelk.createDocument().withIdentifier(identifier).withData(r).withContentType("application/json")

            #print "Sparar dokument i whelken daaraa"
            uri = whelk.store(mydoc)

if _in_console:
    print r
