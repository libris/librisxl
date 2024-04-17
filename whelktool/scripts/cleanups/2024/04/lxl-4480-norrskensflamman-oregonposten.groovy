var OREGONPOSTEN_ID = 'https://libris.kb.se/4ngsxq4g22nlgbn#it'
var NORRSKENSFLAMMAN_ID = 'https://libris.kb.se/fzr371qr0cl7pgp#it'

String where = """
  collection = 'bib' 
  AND deleted = false
  AND data#>'{@graph,1,isIssueOf}' @> '[{"@id": ${OREGONPOSTEN_ID}]'
"""

selectBySqlWhere(where) { bib ->
    def (_, mainEntity) = bib.graph
    def mainTitle = getAtPath(mainEntity, ['hasTitle', '0', 'mainTitle'], "")

    if (mainEntity.isIssueOf == [['@id': OREGONPOSTEN_ID]] && mainTitle.startsWith("NORRSKENSFLAMMAN ")) {
        mainEntity.isIssueOf = [['@id': NORRSKENSFLAMMAN_ID]]
        bib.scheduleSave(loud: true)
    }
}