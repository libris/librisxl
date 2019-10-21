/*
 * See LXL-2730 for more info.
 */

scheduledForChange = getReportWriter("scheduled-for-change")
codeIsAListCounter = getReportWriter("code-is-a-list-counter")

query = """collection = 'bib'
        AND data#>>'{@graph,2,@type}' = 'Text'
        AND data#>>'{@graph,2,genreForm}' LIKE '%"NotFictionNotFurtherSpecified"%'"""

selectBySqlWhere(query, silent: false) { bib ->
    classif = bib.graph[2].classification

    classif = classif instanceof Map ? [classif] : classif

    classif?.each {
        def type = it.'@type' as String

        if (it?.code instanceof List) {
            codeIsAListCounter.println "Code is a list for ${bib.graph[0][ID]}"
        }
        code = it?.code instanceof Map ? [it.code] : it.code

        if (type == "Classification") {
            def inSchemeCode = it.inScheme?.code as String
            if (inSchemeCode && inSchemeCode == "kssb" && it.inScheme?.'@type' == "ConceptScheme") {
                if (code && code.all { c -> c.startsWith("H") || c.startsWith("uH") }) {

                    scheduledForChange.println "Fiction_${bib.graph[0][ID]}"
                }
            }
        }
    }
}

