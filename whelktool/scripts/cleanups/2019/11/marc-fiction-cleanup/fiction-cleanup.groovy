/*
 * See LXL-2730 for more info.
 */

scheduledForChange = getReportWriter("scheduled-for-change")
report = getReportWriter("report")
failedUpdating = getReportWriter("failed-updating")

NOT_FICTION = "https://id.kb.se/marc/NotFictionNotFurtherSpecified"
SKONLITTERATUR = "https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur"
FICTION = "https://id.kb.se/marc/FictionNotFurtherSpecified"
SUBJECT_PREFIX = "https://id.kb.se/term/sao/"

MARC_FICTION_TYPES = ["https://id.kb.se/marc/ShortStory", //j
                      "https://id.kb.se/marc/HumorSatiresEtc", //h
                      "https://id.kb.se/marc/FictionNotFurtherSpecified", //1
                      "https://id.kb.se/marc/Novel", //f
                      "https://id.kb.se/marc/ComicStrip", //c
                      "https://id.kb.se/marc/Drama", //d
                      "https://id.kb.se/marc/Essay", //e
                      "https://id.kb.se/marc/MixedForms", //m
                      "https://id.kb.se/marc/Poetry"] //p

//Saogf terms with inCollection containing both term/fack and term/skon
GF_WITH_DOUBLE_TERMS = ["https://id.kb.se/term/saogf/St%C3%A5uppkomik",
                        "https://id.kb.se/term/saogf/Humor",
                        "https://id.kb.se/term/saogf/Ordspr%C3%A5k%20och%20tales%C3%A4tt",
                        "https://id.kb.se/term/saogf/Anekdoter",
                        "https://id.kb.se/term/saogf/Ess%C3%A4er",
                        "https://id.kb.se/term/saogf/Samlingsverk"]

query = """collection = 'bib'
        AND data#>>'{@graph,2,@type}' = 'Text'
        AND data#>>'{@graph,1,issuanceType}' = 'Monograph'"""

selectBySqlWhere(query, silent: false) { data ->
    def work = data.graph[2]
    def recordId = data.graph[0][ID]

    if (!everySabClassifcationIsH(work)) {
        if (isSaogfSkonlitteratur(data.whelk, work)
                && !hasAnySubjectAsGenreForm(work, recordId)
                && hasAnyNotFictionGenreForm(work)
                && !hasGfWithBothSkonAndFackTerm(work, recordId)
        ) {
            report.println "Record $recordId with genreForm $work.genreForm and classification: ${work.classification?.code}" +
                    "has broader to $SKONLITTERATUR. Replacing $NOT_FICTION with $FICTION..."
            work.genreForm.removeIf { gf -> gf.'@id' == NOT_FICTION }
            work.genreForm.add(['@id': FICTION])
            scheduledForChange.println "$recordId"
            data.scheduleSave()
        }
        return
    }

    if (hasNoGenreFormField(work)) {
        report.println "No genreForm field for $recordId, creating one and setting it to $FICTION"
        def genreFormValue = []
        genreFormValue.add(['@id': FICTION])
        work.'genreForm' = genreFormValue
        scheduledForChange.println "$recordId"
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update $recordId due to: $e")
        })
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
            report.println "Record $recordId with genreForm $work.genreForm " +
                    "has a marc fiction type, removing $NOT_FICTION..."
        } else {
            report.println "Record $recordId with genreForm $work.genreForm " +
                    "has no marc fiction type. Replacing $NOT_FICTION with $FICTION..."
            work.genreForm.add(['@id': FICTION])
        }
        scheduledForChange.println "$recordId"
        data.scheduleSave()

    } else {
        report.println "Could not remove $NOT_FICTION from $recordId"
    }
}

private boolean everySabClassifcationIsH(work) {
    def classif = work.classification
    classif = classif instanceof Map ? [classif] : classif
    def sabClassifications = classif?.findAll { c -> isSAB(c) }
    return sabClassifications ? sabClassifications?.every { c -> hasClassificationH(c) } : false
}

boolean isSAB(classification) {
    if (isInstanceOf(classification, 'Classification')) {
        def inSchemeCode = classification.inScheme?.code as String
        return inSchemeCode && inSchemeCode == "kssb" && isInstanceOf(classification.inScheme, 'ConceptScheme')
    }
    return false
}

boolean hasClassificationH(classification) {
    def code = classification?.code instanceof String ? [classification.code] : classification.code
    return code && code.every { c -> c.startsWith("H") || c.startsWith("uH") }
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
    return work.genreForm && work.genreForm.any { gf -> whelk.relations.isImpliedBy(SKONLITTERATUR, gf.'@id') }
}

private boolean hasAnySubjectAsGenreForm(work, recordId) {
    if (work.genreForm && work.genreForm.any { gf -> gf.'@id'?.startsWith(SUBJECT_PREFIX)}) {
        report.println "Record $recordId with genreForm $work.genreForm " +
                "has a subject as genreForm: not scheduling for change."
        return true
    } else {
        return false
    }
}

private boolean hasGfWithBothSkonAndFackTerm(work, recordId) {
    if (work.genreForm && work.genreForm.any { gf -> GF_WITH_DOUBLE_TERMS.contains(gf.'@id')}) {
        report.println "Record $recordId with genreForm $work.genreForm " +
                "has genreForm with inCollection containing both term/skon and term/fack: not scheduling for change."
        return true
    } else {
        return false
    }
}

private boolean hasAnyMarcFictionType(work) {
    return work.genreForm.any { gf -> MARC_FICTION_TYPES.contains(gf.'@id') }
}