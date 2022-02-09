/*
Add instanceOf.genreForm saogf/Dagstidningar to listed docs

 */
import whelk.Document

notModified = getReportWriter("not-modified.txt")

selectByIds(new File(scriptDir, 'Libris-id_saogf_Dagstidningar.txt').readLines()) { bib ->
    def (record, thing) = bib.graph
    
    if (thing.issuanceType != 'Serial') {
        notModified.println("${bib.doc.shortId} Wrong issuanceType")
        return
    }

    if (thing.'@type' != 'Electronic') {
        notModified.println("${bib.doc.shortId} Not Electronic")
    }
    
    boolean modified = false


    modified |= addLink(thing, ['instanceOf', 'genreForm'], 'https://id.kb.se/term/saogf/Dagstidningar')
    modified |= addLink(record, ['bibliography'], 'https://libris.kb.se/library/DST')

    if (modified) {
        bib.scheduleSave()
    }
}

boolean addLink(Map data, List path, String uri) {
    Map links = Document._get(path, data) as Set
    def link = [ '@id': datasetUri ]
    boolean modified = links.add(link)
    if (modified) {
        Document._set(path, links as List, data)
    }
    return modified
}