/*
 * See LXL-2730 for more info.
 */

scheduledForChange = getReportWriter("scheduled-for-change")
report = getReportWriter("report")

NOT_FICTION = "https://id.kb.se/marc/NotFictionNotFurtherSpecified"
SKONLITTERATUR = "https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur"
FICTION = "https://id.kb.se/marc/FictionNotFurtherSpecified"

query = """collection = 'bib'
        AND data#>>'{@graph,2,@type}' = 'Text'
        AND data#>>'{@graph,1,issuanceType}' = 'Monograph'"""

selectBySqlWhere(query, silent: false) { data ->
    def work = data.graph[2]
    def recordId = data.graph[0][ID]

    if (!hasOnlyHClassifications(work)) {
        //At least one classification is not H OR there are no classfications
        if (isSaogfSkonlitteratur(data.whelk, work)) {
            work.genreForm.removeIf { gf -> gf.'@id' == NOT_FICTION }
            work.genreForm.add(['@id': FICTION])
            report.println "Record $recordId with genreForm $work.genreForm and classification: ${work.classification?.code}" +
                    "has broader to $SKONLITTERATUR. Replacing $NOT_FICTION with $FICTION..."
        }
        return
    }

    if (hasNoGenreFormField(work)) {
        report.println "No genreForm field for $recordId, creating one and setting it to $FICTION"
        genreFormValue = []
        genreFormValue.add(['@id': FICTION])
        work.'genreForm' = genreFormValue
        scheduledForChange.println "$recordId"
        data.scheduleSave()
        return
    }

    if (!hasAnyNotFictionGenreForm(work)) {
        return
    }

    if (hasOnlyNotFictionGenreForm(work)) {
        report.println "Changing $NOT_FICTION to $FICTION for $recordId"
        work.genreForm[0] = ['@id': FICTION]
        scheduledForChange.println "$recordId"
        data.scheduleSave()
        return
    }

    def wasRemoved = work.genreForm.removeIf { gf -> gf.'@id' == NOT_FICTION }

    if (wasRemoved) {
        if (hasAnyMarcFictionType(work)) {
            report.println "Record $recordId with genreForm $work.genreForm" +
                    "has a marc fiction type, removing $NOT_FICTION..."
        } else {
            work.genreForm.add(['@id': FICTION])
            report.println "Record $recordId with genreForm $work.genreForm" +
                    "has no marc fiction type. Replacing $NOT_FICTION with $FICTION..."
        }
        scheduledForChange.println "$recordId"
        data.scheduleSave()

    } else {
        report.println "Could not remove $NOT_FICTION from $recordId"
    }
}

private boolean hasOnlyHClassifications(work) {
    def classif = work.classification
    classif = classif instanceof Map ? [classif] : classif
    return classif?.every { c -> hasClassificationH(c) }
}

boolean hasClassificationH(classification) {
    def type = classification.'@type' as String
    def code = classification?.code instanceof String ? [classification.code] : classification.code

    if (type == "Classification") {
        def inSchemeCode = classification.inScheme?.code as String
        if (inSchemeCode && inSchemeCode == "kssb" && classification.inScheme?.'@type' == "ConceptScheme") {
            return code && code.every { c -> c.startsWith("H") || c.startsWith("uH") }
        }
    }

    return false
}

private boolean hasAnyNotFictionGenreForm(work) {
    return work.genreForm && work.genreForm.any { gf -> gf.'@id' == NOT_FICTION }
}

private boolean hasOnlyNotFictionGenreForm(work) {
    return work.genreForm && work.genreForm.size() == 1 && work.genreForm[0].'@id' == NOT_FICTION
}

private boolean hasNoGenreFormField(work) {
    return !work.genreForm
}

private boolean isSaogfSkonlitteratur(whelk, work) {
    return work.genreForm.any { gf -> whelk.isImpliedBy(SKONLITTERATUR, gf.'@id') }
}

private boolean hasAnyMarcFictionType(work) {
    def marcFictionTypes = ["https://id.kb.se/marc/ShortStory", //j
                            "https://id.kb.se/marc/HumorSatiresEtc", //h
                            "https://id.kb.se/marc/FictionNotFurtherSpecified", //1
                            "https://id.kb.se/marc/Novel", //f
                            "https://id.kb.se/marc/ComicStrip", //c
                            "https://id.kb.se/marc/Drama", //d
                            "https://id.kb.se/marc/Essay", //e
                            "https://id.kb.se/marc/MixedForms", //m
                            "https://id.kb.se/marc/Poetry"] //p

    return work.genreForm.any { gf -> marcFictionTypes.contains(gf.'@id') }
}