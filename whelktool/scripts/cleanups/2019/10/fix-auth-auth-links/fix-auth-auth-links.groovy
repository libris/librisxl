/*
 *  This script takes broken sameAs links for auth records at any level in the tree for objects such as
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
import datatool.util.DocumentUtil

scheduledForChange = getReportWriter("scheduled-for-change")

selectByCollection('auth') { data ->

    def instance = data.graph[1]

    boolean modified = DocumentUtil.traverse(instance, { object, path ->
        if (object instanceof Map && object.sameAs && path.size() > 0) {
            linkerId = object.sameAs.first()?.'@id' as String
            if (linkerId && linkedObjectIsValid(object, linkerId)) {
                def correctedLink = ['@id': linkerId]
                scheduledForChange.println "Will replace $object with $correctedLink for ${data.graph[0].'@id'} in the path $path"
                return new DocumentUtil.Replace(correctedLink)
            }
        }
    })
    if (modified) {
        data.scheduleSave()
    }
}

private boolean linkedObjectIsValid(linker, linkerId) {
    def linkerType = linker.'@type'
    def linkerPrefLabel = linker.prefLabel

    boolean isValid = false

    selectByIds([linkerId]) { linkedData ->
        def linkedObject = linkedData.graph[1]
        scheduledForChange.println "Linked object: $linkedObject"
        scheduledForChange.println "LinkerType: ${linkerPrefLabel}"
        scheduledForChange.println "LinkerPrefLabel: $linkerType"

        isValid = linkedObject.'@type' == linkerType && linkedObject.prefLabel == linkerPrefLabel
    }

    return isValid
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




