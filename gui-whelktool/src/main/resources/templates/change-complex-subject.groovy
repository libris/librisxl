PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere(
        "data#>'{@graph,1,instanceOf,subject}' @> '[{\"@type\":\"ComplexSubject\"}]'::jsonb OR " +
        "data#>'{@graph,1,subject}' @> '[{\"@type\":\"ComplexSubject\"}]'::jsonb",
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
        bib.scheduleSave(loud: true, onError: { e ->
            failedBibIDs.println("Failed to update ${bib.doc.shortId} due to: $e")
        })
    }
})

boolean fixSubject(Object subject) {

/*
@graph,1,instanceOf
                "subject": [
                {
                    "@id": "https://id.kb.se/term/sao/1940-talet"
                },
                {
                    "@type": "ComplexSubject",
                    "sameAs": [
                        {
                            "@id": "https://id.kb.se/term/sao/S%C3%A4kerhetspolitik--historia"
                        }
                    ],
                    "inScheme": {
                        "@id": "https://id.kb.se/term/sao"
                    },
                    "prefLabel": "S\u00e4kerhetspolitik--historia",
                    "termComponentList": [
                        {
                            "@type": "Topic",
                            "prefLabel": "S\u00e4kerhetspolitik"
                        },
                        {
                            "@type": "TopicSubdivision",
                            "prefLabel": "historia"
                        }
                    ]
                },

 */

    // FROMTERM
    // TOTERM

    boolean fixedSameAs = false
    boolean fixedPrefLabel = false
    boolean fixedTermComponentList = false

    if (subject["@type"] == "ComplexSubject") {
        //sameAs // Is there something at that ID ? NO (placeholder)!

        if (subject.sameAs != null && subject.sameAs instanceof List) {
            for (Object ref : subject.sameAs) {
                if (ref["@id"].contains("FROMTERM")) {
                    ref["@id"] = ref["@id"].replace("FROMTERM", "TOTERM")
                    fixedSameAs = true
                }
            }
        }

        if (subject.prefLabel != null && subject.prefLabel instanceof String) {
            subject.prefLabel = subject.prefLabel.replace("FROMTERM", "TOTERM")
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