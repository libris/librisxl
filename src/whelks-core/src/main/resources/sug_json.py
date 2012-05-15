#!/usr/bin/env python

import requests, json, io, sys

def transform(a_json):
    sug_json = {}

    for f in a_json['fields']:
        for k, v in f.items():
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
        urlbase = "http://libris.kb.se/xsearch?query=forf:(#100#)%20spr:swe&format=json"
        xreply = requests.get(urlbase.replace("#100#", f_100))
        reply = json.loads(xreply.text)
        sug_json['records'] = reply['xsearch']['records']

        top_3 = reply['xsearch']['list'][:3]
        top_titles = {}
        for p in top_3:
            top_titles[p['identifier']] = p['title']
        sug_json['top_titles'] = top_titles


    except:
        0
    return sug_json
        


if __name__ == "__main__":
    #print "jorasatte ...\n\n"
    data = sys.stdin.read()
    sug_json = transform(json.loads(data))
    sys.stdout.write(json.dumps(sug_json))
    #print "\n\n... typ?"



