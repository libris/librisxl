/**
 * LXL-4441
 *
 * Remove incorrect data originally coming from local works in Elib records and consequently ended up on linked works in
 * the automatic work merging.
 * The terms to remove are:
 *  - "https://id.kb.se/term/barngf/Barn-%20och%20ungdomslitteratur" from genreForm
 *  - "https://id.kb.se/marc/Juvenile" from intendedAudience
 * unless coming from a non-Elib record, in that case keep.
 */

BARN_UNGDOM = "https://id.kb.se/term/barngf/Barn-%20och%20ungdomslitteratur"
MARC_JUVENILE = "https://id.kb.se/marc/Juvenile"

def where = """
    collection = 'auth'
    AND deleted = false
    AND data#>'{@graph,0,hasChangeNote}' @> '[{"tool": {"@id":"https://id.kb.se/generator/mergeworks"}}]'
"""

selectBySqlWhere(where) { workDocItem ->
    def (record, work) = workDocItem.graph
    def hasMarcJuv = hasMarcJuvenile(work)
    def hasBarnUng = hasBarnUngdom(work)

    if (hasMarcJuv || hasBarnUng) {
        def anyElib = false
        def anyNonElibBarnUngdom = false
        def anyNonElibMarcJuvenile = false

        selectByIds(record['derivedFrom'].collect { it['@id'] }) { dfDocItem ->
            def versionBeforeWorkExtraction = dfDocItem.getVersions().reverse().find { !it.data['@graph'][1]['instanceOf']['@id'] }
            def (dfRecord, dfInstance) = versionBeforeWorkExtraction.data['@graph']

            if (isElib(dfRecord)) {
                anyElib = true
                return
            }

            anyNonElibBarnUngdom |= hasBarnUngdom(dfInstance['instanceOf'])
            anyNonElibMarcJuvenile |= hasMarcJuvenile(dfInstance['instanceOf'])
        }

        if (anyElib) {
            if (hasBarnUng && !anyNonElibBarnUngdom) {
                work['genreForm'].remove(['@id': BARN_UNGDOM])
                if (work['genreForm'].isEmpty()) {
                    work.remove('genreForm')
                }
                workDocItem.scheduleSave()
            }
            if (hasMarcJuv && !anyNonElibMarcJuvenile) {
                work['genreForm'].remove(['@id': MARC_JUVENILE])
                if (work['intendedAudience'].isEmpty()) {
                    work.remove('intendedAudience')
                }
                workDocItem.scheduleSave()
            }
        }
    }
}

def hasBarnUngdom(Map work) {
    asList(work['genreForm']).any { it['@id'] == BARN_UNGDOM }
}

def hasMarcJuvenile(Map work) {
    asList(work['intendedAudience']).any { it['@id'] == MARC_JUVENILE }
}

def isElib(Map record) {
    return asList(record['identifiedBy']).any { it['value'] =~ "^Elib" }
}