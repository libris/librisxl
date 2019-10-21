/*
 * This script changes the g/f from $NOT_FICTION to $SKONLITTERATUR for ~200k bibliographic records
 * that have only SAB classifications starting with code H or uH.
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
    classif = work.classification
    classif = classif instanceof Map ? [classif] : classif

    classif?.each {
        def type = it.'@type' as String
        code = it?.code instanceof Map ? [it.code] : it.code

        if (type == "Classification") {
            def inSchemeCode = it.inScheme?.code as String
            if (inSchemeCode && inSchemeCode == "kssb" && it.inScheme?.'@type' == "ConceptScheme") {
                if (code && code.all { c -> c.startsWith("H") || c.startsWith("uH") }) {
                    if (work.genreForm && work.genreForm.length == 1 && work.genreForm[0] == NOT_FICTION) {
                        scheduledForChange.println "${data.graph[0][ID]}"
                        work.genreForm[0] = SKONLITTERATUR
                        data.scheduleSave()
                    }
                }
            }
        }
    }
}

