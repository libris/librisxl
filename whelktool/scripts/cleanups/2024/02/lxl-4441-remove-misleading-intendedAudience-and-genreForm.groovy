/**
 * Remove incorrect data originally coming from local works in Elib records and consequently ended up on linked works in
 * the automatic work merging.
 *
 * The terms to remove are:
 *  - Any https://id.kb.se/term/barngf/... term from genreForm
 *  - Any https://id.kb.se/term/barn/... term from subject
 *  - https://id.kb.se/marc/Juvenile from intendedAudience
 *  - Local entities with specific age range (e.g. "9-12 år") from intendedAudience
 *
 * Remove only if all these terms came from the same Elib record.
 * If multiple sources or another source than Elib, keep.
 */

BARN_NS = "https://id.kb.se/term/barn/"
BARN_GF_NS = "https://id.kb.se/term/barngf/"
MARC_JUVENILE = "https://id.kb.se/marc/Juvenile"

def where = """
    collection = 'auth'
    AND deleted = false
    AND data#>'{@graph,0,hasChangeNote}' @> '[{"tool": {"@id":"https://id.kb.se/generator/mergeworks"}}]'
"""

selectBySqlWhere(where) {
    try {
        process(it)
    } catch (Exception e) {
        println("Unable to process record ${it.doc.shortId} due to $e")
    }
}

def process(workDocItem) {
    def (record, work) = workDocItem.graph

    def hasMarcJuv = hasMarcJuvenile(work)

    if (hasMarcJuvenile(work) || hasBarnGf(work) || hasBarnSubject(work)) {
        def fromElib = 0
        def fromOther = 0

        selectByIds(record['derivedFrom'].collect { it['@id'] }) { dfDocItem ->
            def versionBeforeWorkExtraction = dfDocItem.getVersions().reverse().find {
                it.data['@graph'][1]['instanceOf'].with { it && !it['@id'] }
            }
            def (dfRecord, dfInstance) = versionBeforeWorkExtraction.data['@graph']
            def dfWork = dfInstance['instanceOf']

            if (hasMarcJuvenile(dfWork) || hasBarnGf(dfWork) || hasBarnSubject(dfWork)) {
                if (isElib(dfRecord)) {
                    fromElib += 1
                } else {
                    fromOther += 1
                }
            }
        }

        if (fromElib == 1 && fromOther == 0) {
            work['intendedAudience']?.removeAll {
                if (it['@id'] == MARC_JUVENILE || it['label'] in ['9-12 år', '6-9 år', '12-15 år', '3-6 år', '15', '0-3 år']) {
                    incrementStats('intendedAudience', it)
                    return true
                }
            }
            if (work['intendedAudience']?.isEmpty()) {
                work.remove('intendedAudience')
            }

            work['genreForm']?.removeAll {
                if (it['@id']?.startsWith(BARN_GF_NS)) {
                    if (hasMarcJuv) {
                        incrementStats('genreForm', it)
                    } else {
                        incrementStats('genreForm - no intendedAudience', it)
                    }
                    return true
                }
            }
            if (work['genreForm']?.isEmpty()) {
                work.remove('genreForm')
            }

            work['subject']?.removeAll {
                if (it['@id']?.startsWith(BARN_NS)) {
                    if (hasMarcJuv) {
                        incrementStats('subject', it)
                    } else {
                        incrementStats('subject - no intendedAudience', it)
                    }
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