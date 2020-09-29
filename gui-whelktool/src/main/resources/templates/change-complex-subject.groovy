PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere( "collection = 'bib' AND (" +
        "data#>'{@graph,1,instanceOf,subject}' @> '[{\"@type\":\"ComplexSubject\"}]'::jsonb OR " +
        "data#>'{@graph,1,subject}' @> '[{\"@type\":\"ComplexSubject\"}]'::jsonb" +
        ")",
        silent: false, { bib ->
    boolean changed = false

    if (bib.graph[1].instanceOf != null &&
            bib.graph[1].instanceOf.subject != null &&
            bib.graph[1].instanceOf.subject instanceof List) {
        for (Object subject : bib.graph[1].instanceOf.subject) {
            changed |= fixSubject(subject)
        }
    }

    if (bib.graph[1].subject != null && bib.graph[1].subject instanceof List) {
        for (Object subject : bib.graph[1].subject) {
            changed |= fixSubject(subject)
        }
    }

    if (changed) {
        scheduledForUpdate.println("${bib.doc.getURI()}")
        bib.scheduleSave(loud: true, onError: { e ->
            failedBibIDs.println("Failed to update ${bib.doc.shortId} due to: $e")
        })
    }
})

boolean fixSubject(Object subject) {
    boolean fixedSameAs = false
    boolean fixedPrefLabel = false
    boolean fixedTermComponentList = false

    if (subject["@type"] == "ComplexSubject") {

        if (subject.sameAs != null && subject.sameAs instanceof List) {
            for (Object ref : subject.sameAs) {
                String encodedFrom = URLEncoder.encode("FROMTERM", "UTF-8")
                String encodedTo = URLEncoder.encode("TOTERM", "UTF-8")
                if (ref["@id"].contains(encodedFrom)) {
                    ref["@id"] = ref["@id"].replaceFirst(encodedFrom, encodedTo)
                    fixedSameAs = true
                }
            }
        }

        if (subject.prefLabel != null && subject.prefLabel instanceof String && subject.prefLabel.startsWith("FROMTERM")) {
            subject.prefLabel = subject.prefLabel.replaceFirst("FROMTERM", "TOTERM")
            fixedPrefLabel = true
        }

        if (subject.termComponentList != null && subject.termComponentList instanceof List) {
            if (subject.termComponentList.size() > 0) {
                if (subject.termComponentList[0]["@type"] == "Topic"
                        && subject.termComponentList[0]["prefLabel"] == "FROMTERM") {
                    subject.termComponentList[0]["prefLabel"] = "TOTERM"
                    fixedTermComponentList = true
                }
            }
        }
    }

    return fixedSameAs && fixedPrefLabel && fixedTermComponentList
}