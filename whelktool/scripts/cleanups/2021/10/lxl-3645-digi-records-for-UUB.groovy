String whereBib = """
    collection = 'bib'
    AND data#>>'{@graph,1,@type}' != 'Electronic'
    AND (data#>'{@graph,0,bibliography}' @> '[{"@id":"https://libris.kb.se/library/SPDT"}]'
        OR data#>'{@graph,0,bibliography}' @> '[{"@id":"https://libris.kb.se/library/DIGI"}]')
"""

selectBySqlWhere(whereBib) { bib ->
    def (record, mainEntity) = bib.graph

    boolean modified
    boolean publishedInSweden
    boolean hasHoldForUka

    Map params =
            [
                    'bibliographyCode'      : "DST",
                    'heldById'              : "https://libris.kb.se/library/Unix",
//                    'reproductionComment'   : "",
//                    'reproductionAgentLabel': "",
//                    'year'                  : ""
            ]

    publishedInSweden = mainEntity.publication?.any {
        it.country?."@id" == "https://id.kb.se/country/sw"
    }

    if (!publishedInSweden)
        return

    String whereHold = """
        collection = 'hold' 
        AND deleted = false
        AND data#>>'{@graph,1,itemOf,@id}' = '${mainEntity['@id']}'
        AND data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Uka'
    """

    selectBySqlWhere(whereHold) { hold ->
        Map holdMainEntity = hold.graph[1]
        hasHoldForUka = true
        if (holdMainEntity.uri) {
            params['mediaObjectUri'] = holdMainEntity.uri
            holdMainEntity.remove('uri') // Shouldn't be here?!
        }
    }

    if (hasHoldForUka) {
        createDigitalRepresentation(bib)
        modified = record.bibliography.remove(['@id': "https://libris.kb.se/library/DIGI"])
        if (modified)
            bib.scheduleSave()
    }
}