import whelk.Document

def OBSOLETE_TYPE_NOTES = ['isni', 'orcid', 'viaf', 'wikidata']

selectByCollection('auth') { auth ->
    def (_record, thing) = auth.graph
    
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
        auth.scheduleSave()
    }
}

List asList(Object o) {
    return o in List ? o : [o]
}