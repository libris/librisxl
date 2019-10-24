/*
 *  This script takes broken links for auth records in 'broader', 'narrower' and 'related' objects such as
 *
 *  { "@type" : "Topic", "sameAs" : [{ "@id" : "https://id.kb.se/term/barn/Vardagsliv" }],
 *  "prefLabel" : "Vardagsliv" }
 *
 *  and repairs them, i.e. transforms them to
 *
 *  { "@id" : "https://id.kb.se/term/barn/Vardagsliv" }
 *
 *
 *  See LXL-2721 for more info.
 *
 */

PrintWriter scheduledForChange = getReportWriter("scheduled-for-change")

selectBySqlWhere("collection = 'auth'") { data ->

    def instance = data.graph[1]

    def relations = [instance.related,
                     instance.broader,
                     instance.narrower]

    relations.each { relation ->
        relation.each { brokenLinker ->

            def sameAs = brokenLinker?.sameAs as List

            if (sameAs) {
                linkerType = brokenLinker.'@type'
                linkerPrefLabel = brokenLinker.'prefLabel'
                linkerId = sameAs.first()?.'@id'

                String query = """id in (select id from lddb
                where data#>>'{@graph,1,@id}' = '${linkerId}') and
                collection = 'auth'"""

                selectBySqlWhere(query, silent: false) { auth ->
                    if (auth.'@type' == linkerType && auth.'prefLabel' == linkerPrefLabel) {
                        correctedLink = ['@id': linkerId]
                        brokenLinker = correctedLink
                        scheduledForChange.println "${data.doc.getURI()}"
                        data.scheduleSave()
                    }
                }
            }
        }
    }
}


