/**
 * Add meta.bibliography QLIT to all instances held by QLIT
 * 
 * See LXL-3850 for more information
 */

String LIBRARY_URI = 'https://libris.kb.se/library/QLIT'

where = """
    id in (
        SELECT l.id FROM lddb l, lddb__identifiers i 
        WHERE l.id = i.id 
        AND EXISTS (
            SELECT id AS id2 FROM lddb 
            WHERE data #>> '{@graph,1,heldBy,@id}' = '$LIBRARY_URI' 
            AND data #>> '{@graph,1,itemOf,@id}' = i.iri
        )
    )
"""

selectBySqlWhere(where) { bib -> 
    def (record, thing) = bib.graph

    def bibliography = asSet(record.bibliography)
    if (bibliography.add(['@id': LIBRARY_URI])) {
        record.bibliography = bibliography
        bib.scheduleSave(loud: true)
    }
}

// -------------------------------------------------------------------------------

static List asList(Object o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}

static Set asSet(Object o) {
    asList(o) as Set
}