import whelk.Document

OBSOLETE_TYPE_NOTES = [
        'ansi',
        'doi',
        'hdl',
        'isan',
        'isni',
        'iso',
        'istc',
        'iswc',
        'orcid',
        'urn',
        'viaf',
        'wikidata',
]

selectByCollection('auth') { auth ->
    process(auth)
}

selectByCollection('bib') { bib ->
    process(bib)
}

void process(docItem) {
    def (_record, thing) = docItem.graph
    
    boolean needsUpdate = false
    thing.identifiedBy?.with {
        asList(it).forEach { Map id ->
            id.typeNote?.with { String tn ->
                if (OBSOLETE_TYPE_NOTES.contains(tn)) {
                    needsUpdate = true
                }
            }
            asList(it).findAll { Document.&isIsni || Document.&isOrcid }.forEach { Map isni ->
                if (((String) isni.value)?.contains(' ')) {
                    needsUpdate = true
                }
            }
        }
    }

    if (needsUpdate) {
        docItem.scheduleSave()
    }
}

List asList(Object o) {
    return o in List ? o : [o]
}