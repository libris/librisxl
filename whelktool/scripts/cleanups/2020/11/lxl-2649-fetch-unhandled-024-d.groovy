/**
 * Fetch 024#d from unhandled and add as hasNote in identifiedBy.
 * Fetch 024#c from unhandled add map it to acquisitionTerms in identifiedBy.
 *
 * See LXL-2649 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' AND deleted = false AND data#>>'{@graph,0,_marcUncompleted}' LIKE '%\"024\"%'"

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph
    boolean changed = false

    if (!instance.identifiedBy)
        return

    asList(record._marcUncompleted).removeAll { uncompleted ->
        boolean toBeRemoved = false

        if (uncompleted instanceof Map && uncompleted.keySet().contains("024")) {
            if (!("c" in asList(uncompleted["_unhandled"])) && !("d" in asList(uncompleted["_unhandled"])))
                return false

            Map _024 = [:]
            asList(uncompleted["024"].subfields).each { it ->
                _024.a = it.a ?: _024.a
                _024.c = it.c ?: _024.c
                _024.d = it.d ?: _024.d
            }

            if (_024.a && (_024.c || _024.d)) {
                // See if we can match _024.a against an identifiedBy.value in the instance
                instance.identifiedBy?.each { identifiedBy ->
                    if (identifiedBy.value && identifiedBy.value == _024.a) {
                        // If we had a "d" in unhandled, add it as a note
                        if (_024.d) {
                            if (!identifiedBy.hasNote)
                                identifiedBy.hasNote = []
                            else if (!(identifiedBy.hasNote instanceof List))
                                identifiedBy.hasNote = [identifiedBy.hasNote]

                            identifiedBy.hasNote << ["@type": "Note", label: _024.d]
                        }

                        // If we had a "c" in unhandled, map it to acquisitionTerms
                        if (_024.c)
                            identifiedBy.acquisitionTerms = _024.c

                        changed = true
                        toBeRemoved = true
                    }
                }
            }
            return toBeRemoved
        }
        return false
    }

    if (changed) {
        // Clean up _marcUncompleted if possible
        if (record._marcUncompleted instanceof Map || (record._marcUncompleted instanceof List && record._marcUncompleted.size() == 0))
            record.remove("_marcUncompleted")

        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

private List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}
