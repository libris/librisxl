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
report = getReportWriter("report")

selectByCollection('auth') { data ->
    def instance = data.graph[1]

    boolean modified = DocumentUtil.traverse(instance, { object, path ->
        if (object instanceof Map && object.sameAs && path.size() > 0) {
            def linkerId = object.sameAs.first()?.'@id' as String
            if (linkerId && linkedObjectIsValid(object, linkerId)) {
                def correctedLink = ['@id': linkerId]
                report.println "Will replace $object with $correctedLink for ${data.graph[0].'@id'} in the path $path"
                return new DocumentUtil.Replace(correctedLink)
            }
        }
    })
    if (modified) {
        scheduledForChange.println "${data.graph[0].'@id'}"
        data.scheduleSave()
    }
}

private boolean linkedObjectIsValid(Map linker, linkerId) {
    boolean allKeyValuePairsMatch = false

    selectByIds([linkerId]) { linkedData ->
        def linkedObject = linkedData.graph[1] as Map
        report.println "========"
        report.println "Linked object: $linkedObject"
        report.println "Linker: $linker"

        if (!isInstanceOf(linkedObject, linker.'@type')) {
            report.println "Type mismatch:"
            report.println "${linkedObject.'@type'} not an instance of ${linker.'@type'}"
            return
        }

        allKeyValuePairsMatch = linker.every { linkerKey, linkerValue ->
            if (linkerKey == 'sameAs' || linkerKey == '@type') {
                return true
            }
            if (linkedObject[linkerKey] != linkerValue) {
                report.println "Link value mismatch:"
                report.println "LinkerValue: $linkerValue"
                report.println "LinkedValue: ${linkedObject[linkerKey]}"
                return false
            }
            report.println "Matching linkerValue: $linkerValue"
            return true
        }
    }

    return allKeyValuePairsMatch
}