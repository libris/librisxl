import whelk.Document

Set<String> OBSOLETE_TYPE_NOTES = [
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
] as Set


def where = """
    data #>> '{@graph,1,identifiedBy}' LIKE '%typeNote%' 
    AND (collection = 'bib' OR collection = 'auth') 
    AND deleted = false
"""
     
selectBySqlWhere(where) { docItem ->
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
        // document normalizer does all the changes
        docItem.scheduleSave()
    }
}

List asList(Object o) {
    return o in List ? o : [o]
}