# NEVER RUN THIS TEST CASE ON AN ENVIRONMENT WHERE THE DATA MATTERS!
# These tests are not portable.

import os
import json
import xml.etree.ElementTree as ET

base_uri = 'http://kblocalhost.kb.se:5000/'
export_url = 'http://localhost:8580/marc_export/'
    
## Util-stuff

pendingSql = ""
batchSql = True

def queueSql(sql):
    global pendingSql
    pendingSql += sql
    if not batchSql:
        flushSql()

def flushSql():
    global pendingSql
    if pendingSql:
        os.system("psql whelk_dev -c '{}'".format(pendingSql))
        pendingSql = ""

def reset():
    queueSql("delete from lddb__identifiers where id in (select id from lddb where changedIn = $$integtest$$);")
    queueSql("delete from lddb__versions where changedIn = $$integtest$$;")
    queueSql("delete from lddb__dependencies where id in (select id from lddb where changedIn = $$integtest$$);")
    queueSql("delete from lddb where changedIn = $$integtest$$;")

def newBib(jsonstring, agent, systemid, timestring):
    jsonstring = jsonstring.replace("TEMPID", systemid)
    jsonstring = jsonstring.replace("TEMPBASEURI", base_uri)
    queueSql("insert into lddb (id, data, collection, changedIn, changedBy, checksum, modified, created) values($${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, jsonstring, 'bib', 'integtest', agent, '0', timestring, timestring))
    queueSql("insert into lddb__versions (id, data, collection, changedIn, changedBy, checksum, modified, created) values($${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, jsonstring, 'bib', 'integtest', agent, '0', timestring, timestring))
    queueSql("insert into lddb__identifiers (id, iri, graphIndex, mainId) values($${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, base_uri+systemid, '0', 'true'))
    queueSql("insert into lddb__identifiers (id, iri, graphIndex, mainId) values($${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, base_uri+systemid+'#it', '1', 'true'))

def newHold(jsonstring, agent, systemid, itemof, sigel, timestring):
    jsonstring = jsonstring.replace("TEMPID", systemid)
    jsonstring = jsonstring.replace("TEMPBASEURI", base_uri)
    jsonstring = jsonstring.replace("TEMPITEMOF", itemof)
    jsonstring = jsonstring.replace("TEMPSIGEL", sigel)
    queueSql("insert into lddb (id, data, collection, changedIn, changedBy, checksum, modified, created) values($${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, jsonstring, 'hold', 'integtest', agent, '0', timestring, timestring))
    queueSql("insert into lddb__versions (id, data, collection, changedIn, changedBy, checksum, modified, created) values($${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, jsonstring, 'hold', 'integtest', agent, '0', timestring, timestring))
    queueSql("insert into lddb__identifiers (id, iri, graphIndex, mainId) values($${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, base_uri+systemid, '0', 'true'))
    queueSql("insert into lddb__identifiers (id, iri, graphIndex, mainId) values($${}$$, $${}$$, $${}$$, $${}$$);".format(systemid, base_uri+systemid+'#it', '1', 'true'))
    queueSql("insert into lddb__dependencies (id, relation, dependsOnId) values($${}$$, $${}$$, $${}$$);".format(systemid, 'itemOf', itemof))

def updateRecord(agent, systemid, timestring):
    queueSql("insert into lddb__versions (id, data, collection, changedIn, checksum, changedBy, modified) select id, data, collection, changedIn, checksum, $${}$$ as changedBy, $${}$$ as modified from lddb where id = $${}$$;".format(agent, timestring, systemid))
    queueSql("update lddb set modified = $${}$$, changedBy = $${}$$ where id = $${}$$;".format(timestring, agent, systemid))

def relinkHolding(jsonstring, systemid, itemof, sigel):
    jsonstring = jsonstring.replace("TEMPID", systemid)
    jsonstring = jsonstring.replace("TEMPBASEURI", base_uri)
    jsonstring = jsonstring.replace("TEMPITEMOF", itemof)
    jsonstring = jsonstring.replace("TEMPSIGEL", sigel)
    queueSql("update lddb set data = $${}$$ where id = $${}$$;".format(jsonstring, systemid))

def setDeleted(systemid):
    queueSql("update lddb set deleted = true where id = $${}$$;".format(systemid))

def doExport(fromTime, toTime, profileName, exportDeleted=False, virtualDeletions=False):
    flushSql()
    deleted="ignore"
    if exportDeleted:
        deleted="export"
    virtual="false"
    if virtualDeletions:
        virtual="true"

    print('curl -XPOST "{}?from={}&until={}&deleted={}&virtualDelete={}" --data-binary @./testdata/profiles/{}.properties > export.dump'.format(export_url, fromTime, toTime, deleted, virtual, profileName))
    os.system('curl -XPOST "{}?from={}&until={}&deleted={}&virtualDelete={}" --data-binary @./testdata/profiles/{}.properties > export.dump'.format(export_url, fromTime, toTime, deleted, virtual, profileName))

def assertExported(record001, failureMessage):
    with open('export.dump') as fh:
        dump = fh.read()
    if not dump:
        failedCases.append(failureMessage)
        return
    xmlDump = ET.fromstring(dump)
    for elem in xmlDump.findall("{http://www.loc.gov/MARC21/slim}record/{http://www.loc.gov/MARC21/slim}controlfield[@tag='001']"):
        if elem.text == record001:
            return
    failedCases.append(failureMessage)

def assertNotExported(record001, failureMessage):
    with open('export.dump') as fh:
        dump = fh.read()
    if not dump:
        return
    xmlDump = ET.fromstring(dump)
    for elem in xmlDump.findall("{http://www.loc.gov/MARC21/slim}record/{http://www.loc.gov/MARC21/slim}controlfield[@tag='001']"):
        if elem.text == record001:
            failedCases.append(failureMessage)
            return
    return
    
## Init

with open('testdata/bib0.jsonld') as fh:
    bibtemplate = fh.read()
with open('testdata/bibelectronic.jsonld') as fh:
    bibelectronictemplate = fh.read()
with open('testdata/bibfiction.jsonld') as fh:
    bibfiction = fh.read()
with open('testdata/hold0.jsonld') as fh:
    holdtemplate = fh.read()

failedCases = []
    
########## TESTCASES ##########

# Normal new bib and hold should show up in export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertExported("tttttttttttttttt", "Test 1")

# Only new hold in interval, bib should be exported
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertExported("tttttttttttttttt", "Test 2")

# Only bib was updated, bib should be exported
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertExported("tttttttttttttttt", "Test 3")

# Only hold was updated, bib should be exported
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertExported("tttttttttttttttt", "Test 3-2")

# New bib without hold, should be exported when locations=*
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "default_ALL")
assertExported("tttttttttttttttt", "Test 4")

# holdtype=none must not result in empty exports
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "hold_none_SEK")
assertExported("tttttttttttttttt", "Test 5")

# New bib with ony hold for other sigel, should not be exported
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "INTESEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertNotExported("tttttttttttttttt", "Test 6")

# biboperators=SEK and bib-update by INTESEK should not lead to export
# Normal new bib and hold should show up in export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("INTESEK", "tttttttttttttttt", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_SEK")
assertNotExported("tttttttttttttttt", "Test 7")

# holdoperators=SEK and hold-update by INTESEK should not lead to export
# Normal new bib and hold should show up in export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("INTESEK", "hhhhhhhhhhhhhhhh", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_SEK")
assertNotExported("tttttttttttttttt", "Test 8")

# *create=on, *update=off and hold-update should not lead to export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_only_create_SEK")
assertNotExported("tttttttttttttttt", "Test 9")

# *create=on, *update=off and hold create should lead to export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_only_create_SEK")
assertExported("tttttttttttttttt", "Test 10")

# *create=off, *update=on and hold-update should lead to export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_only_update_SEK")
assertExported("tttttttttttttttt", "Test 11")

# *create=off, *update=on and bib-update should lead to export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
updateRecord("SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_only_update_SEK")
assertExported("tttttttttttttttt", "Test 12")

# *create=off, *update=on and no updates should not lead to export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_operator_only_update_SEK")
assertNotExported("tttttttttttttttt", "Test 13")

# Relinking holding should export both new and old bib
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newBib(bibtemplate, "SEK", "bbbbbbbbbbbbbbbb", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
relinkHolding(holdtemplate, "hhhhhhhhhhhhhhhh", "bbbbbbbbbbbbbbbb", "SEK")
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertExported("tttttttttttttttt", "Test 14-1")
assertExported("bbbbbbbbbbbbbbbb", "Test 14-2")

# Relinking holding multiple times inside update interval should export all bibs linked to inside the interval and the last before the interval
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newBib(bibtemplate, "SEK", "bbbbbbbbbbbbbbbb", "2150-01-01 12:00:00")
newBib(bibtemplate, "SEK", "ffffffffffffffff", "2150-01-01 12:00:00")
newBib(bibtemplate, "SEK", "gggggggggggggggg", "2150-01-01 12:00:00")
newBib(bibtemplate, "SEK", "rrrrrrrrrrrrrrrr", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 13:00:00")
relinkHolding(holdtemplate, "hhhhhhhhhhhhhhhh", "bbbbbbbbbbbbbbbb", "SEK") # before the interval
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2151-01-01 12:00:00")
relinkHolding(holdtemplate, "hhhhhhhhhhhhhhhh", "ffffffffffffffff", "SEK") # inside the interval
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 13:00:00")
relinkHolding(holdtemplate, "hhhhhhhhhhhhhhhh", "gggggggggggggggg", "SEK") # inside the interval
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 14:00:00")
relinkHolding(holdtemplate, "hhhhhhhhhhhhhhhh", "rrrrrrrrrrrrrrrr", "SEK") # after the interval
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2350-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T18:00:00Z", "bare_SEK")
assertNotExported("tttttttttttttttt", "Test 14-3")
assertExported("bbbbbbbbbbbbbbbb", "Test 14-4")
assertExported("ffffffffffffffff", "Test 14-5")
assertExported("gggggggggggggggg", "Test 14-6")
assertNotExported("rrrrrrrrrrrrrrrr", "Test 14-7")

# Hold was deleted and holddelete=on, bib should be exported
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2150-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2150-01-01 12:00:00")
setDeleted("hhhhhhhhhhhhhhhh")
updateRecord("SEK", "hhhhhhhhhhhhhhhh", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertExported("tttttttttttttttt", "Test 15")

# Deleted bib should not be exported when in ignore-mode
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
setDeleted("tttttttttttttttt")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK")
assertNotExported("tttttttttttttttt", "Test 16")

# Deleted bib should be exported when deleted=export
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
setDeleted("tttttttttttttttt")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK", exportDeleted=True)
assertExported("tttttttttttttttt", "Test 17")

# Virtual delete when no holdings left
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
setDeleted("hhhhhhhhhhhhhhhh")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "bare_SEK", exportDeleted=True, virtualDeletions=True)
assertExported("tttttttttttttttt", "Test 18")

# Status = off, should not result in any data
reset()
newBib(bibtemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "status_off")
assertNotExported("tttttttttttttttt", "Test 19")

# New electronic should not export when efilter=on
reset()
newBib(bibelectronictemplate, "SEK", "tttttttttttttttt", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhhhh", "tttttttttttttttt", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "efilter_SEK")
assertNotExported("tttttttttttttttt", "Test 20")

# New fiction should not export when fictionfilter=on
reset()
newBib(bibfiction, "SEK", "tttttttttttttt21", "2250-01-01 12:00:00")
newHold(holdtemplate, "SEK", "hhhhhhhhhhhhhh21", "tttttttttttttt21", "SEK", "2250-01-01 12:00:00")
doExport("2250-01-01T10:00:00Z", "2250-01-01T15:00:00Z", "fiction_SEK")
assertNotExported("tttttttttttttt21", "Test 21")

########## SUMMARY ##########

if not failedCases:
    print("*** ALL TESTS OK!")
else:
    print("*** THERE WERE FAILED TESTS:")
    for message in failedCases:
        print(message)
