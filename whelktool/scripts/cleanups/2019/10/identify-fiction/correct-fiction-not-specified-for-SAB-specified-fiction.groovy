/*
 * See LXL-2730 for more info.
 */

scheduledForChange = getReportWriter("scheduled-for-change")
report = getReportWriter("report")

NOT_FICTION = "https://id.kb.se/marc/NotFictionNotFurtherSpecified"
SKONLITTERATUR = "https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur"

query = """collection = 'bib'
        AND data#>>'{@graph,2,@type}' = 'Text'"""

selectBySqlWhere(query, silent: false) { data ->
    def work = data.graph[2]
    def recordId = data.graph[0][ID]

    if (!hasOnlyHClassifications(work) || !hasAnyNotFictionGenreForm(work)) {
        return
    }

    if (hasOnlyNotFictionGenreForm(work)) {
        report.println "Changing $NOT_FICTION to $SKONLITTERATUR for $recordId"
        work.genreForm[0] = ['@id' : SKONLITTERATUR]
        scheduledForChange.println "$recordId"
        data.scheduleSave()
    }

    if (hasNoGenreFormField(work)) {
        report.println "No genreForm field for $recordId, creating one and setting it to $SKONLITTERATUR"
        work.genreForm[0] = ['@id' : SKONLITTERATUR]
        scheduledForChange.println "$recordId"
        data.scheduleSave()
    }

    if (hasAnyBroaderRelationToSkon(work)) {
        def wasRemoved = work.genreForm.removeIf { gf -> gf.'@id' == NOT_FICTION }
        if (wasRemoved) {
            report.println """Record $recordId with genreForm $work.genreForm 
                has a broader relation to $SKONLITTERATUR, removing $NOT_FICTION..."""
            scheduledForChange.println "$recordId"
            data.scheduleSave()
        } else {
            report.println "Could not remove $NOT_FICTION from $recordId"
        }
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

private boolean hasAnyBroaderRelationToSkon(work) {
    //For testing purposes, use broader cache when available
    def narrowerToFictionFirstLevel = ["https://id.kb.se/term/saogf/Allegorier",
    "https://id.kb.se/term/saogf/Arbetarskildringar",
    "https://id.kb.se/term/saogf/Barndomsskildringar",
    "https://id.kb.se/term/saogf/Biografiska%20skildringar",
    "https://id.kb.se/term/saogf/Chick%20lit",
    "https://id.kb.se/term/saogf/Deckare",
    "https://id.kb.se/term/saogf/Dialektlitteratur",
    "https://id.kb.se/term/saogf/Djurskildringar",
    "https://id.kb.se/term/saogf/Dokument%C3%A4ra%20skildringar",
    "https://id.kb.se/term/saogf/Dramatik",
    "https://id.kb.se/term/saogf/Episka%20skildringar",
    "https://id.kb.se/term/saogf/Erotiska%20skildringar",
    "https://id.kb.se/term/saogf/Ess%C3%A4er",
    "https://id.kb.se/term/saogf/Experimentell%20sk%C3%B6nlitteratur",
    "https://id.kb.se/term/saogf/Fabler",
    "https://id.kb.se/term/saogf/Familjeskildringar",
    "https://id.kb.se/term/saogf/Fanfiction",
    "https://id.kb.se/term/saogf/Fantasy",
    "https://id.kb.se/term/saogf/Feelgood",
    "https://id.kb.se/term/saogf/Folkdiktning",
    "https://id.kb.se/term/saogf/Framtidsskildringar",
    "https://id.kb.se/term/saogf/F%C3%B6ljetonger",
    "https://id.kb.se/term/saogf/Herdediktning",
    "https://id.kb.se/term/saogf/Historiska%20skildringar",
    "https://id.kb.se/term/saogf/Humor",
    "https://id.kb.se/term/saogf/Kontrafaktisk%20historia",
    "https://id.kb.se/term/saogf/Krigsskildringar",
    "https://id.kb.se/term/saogf/K%C3%A4rleksskildringar",
    "https://id.kb.se/term/saogf/Magisk%20realism",
    "https://id.kb.se/term/saogf/Noveller",
    "https://id.kb.se/term/saogf/Parafraser",
    "https://id.kb.se/term/saogf/Parodier",
    "https://id.kb.se/term/saogf/Pastischer",
    "https://id.kb.se/term/saogf/Psykologiska%20skildringar",
    "https://id.kb.se/term/saogf/Poesi",
    "https://id.kb.se/term/saogf/Politiska%20skildringar",
    "https://id.kb.se/term/saogf/Ramber%C3%A4ttelser",
    "https://id.kb.se/term/saogf/Relationsskildringar",
    "https://id.kb.se/term/saogf/Religi%C3%B6sa%20skildringar",
    "https://id.kb.se/term/saogf/Riddardiktning",
    "https://id.kb.se/term/saogf/Robinsonader",
    "https://id.kb.se/term/saogf/Romaner",
    "https://id.kb.se/term/saogf/Sagalitteratur",
    "https://id.kb.se/term/saogf/Sagor",
    "https://id.kb.se/term/saogf/Samh%C3%A4llskritiska%20skildringar",
    "https://id.kb.se/term/saogf/Samh%C3%A4llsskildringar",
    "https://id.kb.se/term/saogf/Satir",
    "https://id.kb.se/term/saogf/Science%20fiction",
    "https://id.kb.se/term/saogf/Sedeskildringar",
    "https://id.kb.se/term/saogf/Sjukdomsskildringar",
    "https://id.kb.se/term/saogf/Sj%C3%B6ber%C3%A4ttelser",
    "https://id.kb.se/term/saogf/Skr%C3%A4ck"]
    return work.genreForm.any { gf -> narrowerToFictionFirstLevel.contains(gf.'@id') }
}