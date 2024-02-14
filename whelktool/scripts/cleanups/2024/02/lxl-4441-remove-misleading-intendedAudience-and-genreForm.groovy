/**
 * Remove incorrect data originally coming from local works in Elib records and consequently ended up on linked works in
 * the automatic work merging.
 * The terms to remove are:
 *  - "https://id.kb.se/term/barngf/Barn-%20och%20ungdomslitteratur" from genreForm
 *  - "https://id.kb.se/marc/Juvenile" from intendedAudience
 * unless either coming from a non-Elib record, in that case keep both.
 */

BARN_NS = "https://id.kb.se/term/barn/"
BARN_GF_NS = "https://id.kb.se/term/barngf/"
MARC_JUVENILE = "https://id.kb.se/marc/Juvenile"

def where = """
    collection = 'auth'
    AND deleted = false
    AND data#>'{@graph,0,hasChangeNote}' @> '[{"tool": {"@id":"https://id.kb.se/generator/mergeworks"}}]'
"""

selectBySqlWhere(where) { workDocItem ->
    def (record, work) = workDocItem.graph

    if (hasMarcJuvenile(work) || hasBarnGf(work) || hasBarnSubject(work)) {
        def anyElib = false
        def anyNonElib = false

        selectByIds(record['derivedFrom'].collect { it['@id'] }) { dfDocItem ->
            def versionBeforeWorkExtraction = dfDocItem.getVersions().reverse().find { !it.data['@graph'][1]['instanceOf']['@id'] }
            def (dfRecord, dfInstance) = versionBeforeWorkExtraction.data['@graph']

            if (isElib(dfRecord)) {
                anyElib = true
                return
            }

            def dfWork = dfInstance['instanceOf']

            anyNonElib |= (hasMarcJuvenile(dfWork) || hasBarnGf(dfWork) || hasBarnSubject(dfWork))
        }

        if (anyElib && !anyNonElib) {
            work['intendedAudience']?.removeAll {
                if (it['@id'] == MARC_JUVENILE || it['label'] in ['9-12 år', '6-9 år', '12-15 år', '3-6 år', '15', '0-3 år']) {
                    incrementStats('intendedAudience', it)
                    return true
                }
            }
            if (work['intendedAudience'].isEmpty()) {
                work.remove('intendedAudience')
            }

            work['genreForm']?.removeAll {
                if (it['@id']?.startsWith(BARN_GF_NS)) {
                    incrementStats('genreForm', it)
                    return true
                }
            }
            if (work['genreForm']?.isEmpty()) {
                work.remove('genreForm')
            }

            work['subject']?.removeAll {
                if (it['@id']?.startsWith(BARN_NS)) {
                    incrementStats('subject', it)
                    return true
                }
            }
            if (work['subject']?.isEmpty()) {
                work.remove('subject')
            }

            workDocItem.scheduleSave()
        }
    }
}

def hasBarnSubject(Map work) {
    asList(work['subject']).any { it['@id']?.startsWith(BARN_NS) }
}

def hasBarnGf(Map work) {
    asList(work['genreForm']).any { it['@id']?.startsWith(BARN_GF_NS) }
}

def hasMarcJuvenile(Map work) {
    asList(work['intendedAudience']).any { it['@id'] == MARC_JUVENILE }
}

def isElib(Map record) {
    return asList(record['identifiedBy']).any { it['value'] =~ "^Elib" }
}