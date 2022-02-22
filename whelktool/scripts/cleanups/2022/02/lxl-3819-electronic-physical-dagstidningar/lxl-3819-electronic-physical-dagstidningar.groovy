/*
For tidningar.kb.se serials.

Using id list from VDD database:
- Link electronic/reproduction series to physical series with reproductionOf if missing
- Add tidningar.kb.se bibliography to electronic series
- Set physical thing @type to 'Print'

See LXL-3819 for more information
 */
import whelk.Document

notModified = getReportWriter("not-modified.txt")
badIds = getReportWriter("bad-ids.txt")

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
    
    // Sanity check input
    if (thing.issuanceType != 'Serial') {
        notModified.println("${bib.doc.shortId} Wrong issuanceType")
        return
    }

    if (thing.'@type' != 'Electronic') {
        notModified.println("${bib.doc.shortId} Not Electronic")
        return
    }

    // Link electronic/reproduction to physical if missing
    if (!getAtPath(bib.graph, [1, 'reproductionOf'])) {
        bib.graph[1].reproductionOf = ['@id': electronicToPhysicalId[thing.'@id']]
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