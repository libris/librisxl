/**
 * Fetch date for frequency from unhandled and add to frequency on instance
 *
 * See LXL-2841 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and data#>>'{@graph,0,_marcUncompleted}' LIKE '%\"310\"%'"

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph
    boolean changed = false
    String uncompletedFrequencyLabel = null
    String uncompletedFrequencyDate = null

    // Code borrowed from lxl-2136-musicFormat.groovy
    asList(record._marcUncompleted).each { uncompleted ->
        if (uncompleted instanceof Map && uncompleted.keySet().contains("310")) {
            asList(uncompleted["310"].subfields).each { it ->
                if (it["a"]) {
                    uncompletedFrequencyLabel = it["a"]
                }
                if (it["b"]) {
                    uncompletedFrequencyDate = it["b"]
                }
            }
        }
    }

    if (!uncompletedFrequencyLabel || !uncompletedFrequencyDate || !instance.frequency)
        return

    instance.frequency.each { frequency ->
        if (frequency["@type"] == "Frequency" && frequency.label) {
            // In the affected instances label is usually a list, but sometimes a string
            if (frequency.label instanceof String && frequency.label == uncompletedFrequencyLabel) {
                frequency.date = uncompletedFrequencyDate
                changed = true
            } else if (frequency.label instanceof List) {
                frequency.label.any { label ->
                    if (label == uncompletedFrequencyLabel) {
                        frequency.date = uncompletedFrequencyDate
                        changed = true
                        return
                    }
                }
            }
        }
    }

    if (changed) {
        // Only remove _marcUncompleted stuff if we actually managed to change something
        if (record._marcUncompleted instanceof List) {
            for (int i = record._marcUncompleted.size() - 1; i > -1; --i) {
                def field = record._marcUncompleted[i]
                if (field != null && field["310"] != null) {
                    record._marcUncompleted.remove(i)
                }
            }

            if (record._marcUncompleted.size() == 0) {
                record.remove("_marcUncompleted")
            }
        }

        if (record._marcUncompleted instanceof Map && record._marcUncompleted.containsKey("310")) {
             record.remove("_marcUncompleted")
        }

        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
