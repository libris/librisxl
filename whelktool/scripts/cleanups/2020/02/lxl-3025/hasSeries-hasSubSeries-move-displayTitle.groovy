/*
 * This moves marc:displayText for hasSeries and hasSubSeries. Eg:
 *
 * Instance.hasSeries.Instance.instanceOf.marc:displayText will become
 * Instance.hasSeries.Instance.marc:displayText
 *
 *
 * See LXL-3025 for more info.
 */

PrintWriter failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")

LINK_FIELDS = ['hasSeries', 'hasSubseries']
DISPLAY_TEXT = 'marc:displayText'


selectBySqlWhere("""
        collection = 'bib' AND
        (data#>>'{@graph,1,${LINK_FIELDS[0]}}' LIKE '%${DISPLAY_TEXT}%' OR
         data#>>'{@graph,1,${LINK_FIELDS[1]}}' LIKE '%${DISPLAY_TEXT}%')
    """) { data ->
    def (record, thing, work) = data.graph

    thing.subMap(LINK_FIELDS).each { key, val ->
        moveDisplayTextInLinkField(val, record[ID])
    }

    scheduledForChange.println "Record was updated ${record[ID]}"
    data.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}

void moveDisplayTextInLinkField(linkfieldValue, docId) {
    def displayText
    if (linkfieldValue instanceof Map) {
        displayText = linkfieldValue.instanceOf.remove(DISPLAY_TEXT)
    } else if (linkfieldValue instanceof List) {
        linkfieldValue.each { moveDisplayTextInLinkField(it, docId) }
        return
    }

    if (displayText)
        linkfieldValue[DISPLAY_TEXT] = displayText

    if (linkfieldValue.instanceOf.size() == 1 && linkfieldValue.instanceOf.containsKey(TYPE))
        linkfieldValue.remove('instanceOf')
}