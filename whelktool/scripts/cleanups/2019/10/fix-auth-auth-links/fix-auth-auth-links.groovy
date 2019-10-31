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

scheduledForChange = getReportWriter("scheduled-for-change")

selectBySqlWhere("collection = 'auth'") { data ->

    instance = data.graph[1]

    repairLinksFor(instance.related, "related", data)
    repairLinksFor(instance.broader, "broader", data)
    repairLinksFor(instance.narrower, "narrower", data)
}

private repairLinksFor(relation, name, data) {
    relation.eachWithIndex { brokenLink, ix ->

        linkerType = brokenLink.'@type'
        linkerPrefLabel = brokenLink.'prefLabel'
        linkerId = brokenLink.sameAs?.first()?.'@id'

        if(!linkerId) {
            return
        }

        selectByIds([linkerId]) { linkedData ->
            auth = linkedData.graph[1]
            if (auth.'@type' == linkerType && auth.'prefLabel' == linkerPrefLabel) {
                correctedLink = ['@id': linkerId]
                scheduledForChange.println "Will replace ${relation[ix]} with $correctedLink for ${data.graph[0].'@id'} and relation $name"
                relation[ix] = correctedLink
                data.scheduleSave()
            }
        }
    }
}

//   Handle closeMatch:
//  "closeMatch": [
//    {
//      "@type": "GenreForm",
//      "sameAs": [
//        {
//          "@id": "https://id.kb.se/term/saogf/Animated%20films"
//        }
//      ],
//      "inScheme": {
//        "code": "lcgft",
//        "@type": "ConceptScheme",
//        "sameAs": [
//          {
//            "@id": "https://id.kb.se/term/lcgft"
//          }
//        ]
//      },
//      "prefLabel": "Animated films"
//    }

// "broadMatch": [
//    {
//      "code": "Dofa",
//      "@type": "Classification",
//      "inScheme": {
//        "code": "kssb",
//        "@type": "ConceptScheme",
//        "sameAs": [
//          {
//            "@id": "https://id.kb.se/term/kssb%2F8/"
//          }
//        ],
//        "version": "8"
//      }
//    },




