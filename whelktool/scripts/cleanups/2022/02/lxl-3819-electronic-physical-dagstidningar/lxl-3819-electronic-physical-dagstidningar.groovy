/*
For tidningar.kb.se serials.

Using id list from VDD database:
- Link electronic/reproduction series to physical series with reproductionOf if missing
- Add tidningar.kb.se bibliography to electronic series
- Set physical thing @type to 'Print'

See LXL-3819 for more information
 */
import whelk.Document
import groovy.json.JsonOutput

notModified = getReportWriter("not-modified.txt")
badIds = getReportWriter("bad-ids.txt")
badLinks = getReportWriter("maybe-bad-links.txt")

INPUT_FILE_NAME = 'libris_physical_electronic.tsv'

def TIDNINGAR_BIBLIOGRAPHY = 'https://libris.kb.se/library/TID'

electronicToPhysicalId = [:]

new File(scriptDir, INPUT_FILE_NAME).readLines().each {
    def (physicalId, electronicId) = it.split('\t')
    def eMainId = loadThing(controlNumberToId(electronicId)).'@id'
    def pMainId = loadThing(controlNumberToId(physicalId)).'@id'
    if (eMainId && pMainId) {
        electronicToPhysicalId[eMainId] = pMainId
    }
    else {
        badIds.println("${electronicId}\t${physicalId} : $eMainId --> $pMainId ???")
    }
}

selectByIds(electronicToPhysicalId.keySet().collect()) { bib ->
    def (record, thing) = bib.graph

    def physicalId = electronicToPhysicalId[thing.'@id']
    
    // Sanity check input
    if (thing.issuanceType != 'Serial') {
        notModified.println("${bib.doc.shortId} Wrong issuanceType")
        return
    }

    if (thing.'@type' != 'Electronic') {
        notModified.println("${bib.doc.shortId} Not Electronic")
        return
    }

    // Remove any obsolete otherPhysicalFormat
    def i = asList(thing.otherPhysicalFormat).iterator()
    while (i.hasNext()) {
        def otherPhysicalFormat = i.next()
        
        List controlNumbers = getAtPath(otherPhysicalFormat, ['describedBy', '*', 'controlNumber'], [])
        
        if (controlNumbers.size() > 1) {
            incrementStats('multiple control numbers', controlNumbers)
            continue
        }
        
        def linked = controlNumbers
                .collect(this::controlNumberToId)
                .collect(this::loadThing)
                .findAll {
                    it.'@id' == physicalId
                }

        def maybeBadLinks = linked.findAll { !titles(it).intersect(titles(otherPhysicalFormat)) }
        if (maybeBadLinks) {
            maybeBadLinks.each {
                badLinks.println("""${bib.doc.shortId}
                    ${JsonOutput.prettyPrint(JsonOutput.toJson(otherPhysicalFormat))}
                    
                    -->

                    ${JsonOutput.prettyPrint(JsonOutput.toJson(it))}
                    ==================================================================================\n\n
                    """.stripIndent())
            }
        }
        
        if (linked) {
            i.remove()
            bib.scheduleSave()
        }
    }
    
    if (asList(thing.otherPhysicalFormat).isEmpty()) {
        thing.remove(thing.otherPhysicalFormat)
    }

    // Link electronic/reproduction to physical if missing
    if (!getAtPath(bib.graph, [1, 'reproductionOf'])) {
        bib.graph[1].reproductionOf = ['@id': physicalId]
        bib.scheduleSave()
    }

    // Add tidningar.kb.se bibliography to electronic series
    if (addLink(record, ['bibliography'], TIDNINGAR_BIBLIOGRAPHY)) {
        bib.scheduleSave()
    }
}

// Set physical thing @type to 'Print'
selectByIds(electronicToPhysicalId.values().collect()) { bib ->
    def (record, thing) = bib.graph

    // Sanity check input
    if (thing.issuanceType != 'Serial') {
        notModified.println("${bib.doc.shortId} Wrong issuanceType")
        return
    }

    if (thing.'@type' != 'Print') {
        thing.'@type' = 'Print'
        bib.scheduleSave()
    }
}

def titles(Map thing) {
    getAtPath(thing, ['hasTitle', '*', 'mainTitle'], []).collect { String title -> title.toLowerCase() }
}

//---------------------------------

static def controlNumberToId(String controlNumber) {
    def isXlId = controlNumber.size() > 14
    isXlId
            ? controlNumber
            : 'http://libris.kb.se/resource/bib/' + controlNumber
}

static Object getAtPath(item, Iterable path, defaultTo = null) {
    if(!item) {
        return defaultTo
    }

    for (int i = 0 ; i < path.size(); i++) {
        def p = path[i]
        if (p == '*' && item instanceof Collection) {
            return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
        }
        else if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}


boolean addLink(Map data, List path, String uri) {
    def links = (Document._get(path, data) ?: []) as Set
    def link = [ '@id': uri ]
    boolean modified = links.add(link)
    if (modified) {
        Document._set(path, links as List, data)
    }
    return modified
}

static List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}