/*
 * This script changes the g/f from $NOT_FICTION to $SKONLITTERATUR for ~200k bibliographic records
 * that have only SAB classifications starting with code H or uH. Only looking at records with 1 g/f -term.
 *
 * See LXL-2730 for more info.
 */

scheduledForChange = getReportWriter("scheduled-for-change")

NOT_FICTION = "https://id.kb.se/marc/NotFictionNotFurtherSpecified"
SKONLITTERATUR = "https://id.kb.se/term/saogf/Skönlitteratur" //Use https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur ?

query = """collection = 'bib'
        AND data#>>'{@graph,2,@type}' = 'Text'
        AND data#>>'{@graph,2,genreForm}' LIKE '%"${NOT_FICTION}"%'"""

selectBySqlWhere(query, silent: false) { data ->
    work = data.graph[2]

    if (work.genreForm && work.genreForm.size() == 1 && work.genreForm[0].'@id' == NOT_FICTION) {
        classif = work.classification
        classif = classif instanceof Map ? [classif] : classif
        if (classif?.every { c -> hasClassificationH(c) }) {
            scheduledForChange.println "${data.graph[0][ID]}"
            work.genreForm[0] = SKONLITTERATUR
            data.scheduleSave()
        }
    }
}

boolean hasClassificationH(classification) {
    def type = classification.'@type' as String
    code = classification?.code instanceof String ? [classification.code] : classification.code
    if (type == "Classification") {
        def inSchemeCode = classification.inScheme?.code as String
        if (inSchemeCode && inSchemeCode == "kssb" && classification.inScheme?.'@type' == "ConceptScheme") {
            return code && code.every { c -> c.startsWith("H") || c.startsWith("uH") }
        }
    }
    return false
}