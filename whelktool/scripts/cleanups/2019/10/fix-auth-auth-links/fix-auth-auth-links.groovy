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

private boolean linkedObjectIsValid(Map linker, linkerId) {
    boolean allKeyValuePairsMatch = false

    selectByIds([linkerId]) { linkedData ->
        def linkedObject = linkedData.graph[1] as Map
        scheduledForChange.println "========"
        scheduledForChange.println "Linked object: $linkedObject"
        scheduledForChange.println "Linker: $linker"

        if (!isInstanceOf(linkedObject, linker.'@type')) {
            scheduledForChange.println "Type mismatch:"
            scheduledForChange.println "${linkedObject.'@type'} not an instance of ${linker.'@type'}"
            return
        }

        linker.each { linkerKey, linkerValue ->
            if (linkerKey == 'sameAs' || linkerKey == '@type') {
                return
            }
            if (linkedObject[linkerKey] != linkerValue) {
                scheduledForChange.println "Link value mismatch:"
                scheduledForChange.println "LinkerValue: $linkerValue"
                scheduledForChange.println "LinkedValue: ${linkedObject[linkerKey]}"
                allKeyValuePairsMatch = false
                return
            }
            allKeyValuePairsMatch = true
            scheduledForChange.println "Matching linkerValue: $linkerValue"
        }
    }
    return allKeyValuePairsMatch
}