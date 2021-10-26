import whelk.Document
import whelk.Whelk

String where = """
    (
        collection = 'bib' and data#>'{@graph,1,instanceOf,subject}' @> '[{"@type":"ComplexSubject"}]'
        and data#>>'{@graph,1,instanceOf,subject}' like '%ndian%'
    )
    or
    (
        collection = 'auth' and data#>'{@graph,1,subject}' @> '[{"@type":"ComplexSubject"}]'
        and data#>>'{@graph,1,subject}' like '%ndian%'
    )
"""

selectBySqlWhere(where) { data ->
    def mainEntity = data.graph[1]
    def subjectEntities = []
    if (mainEntity["subject"])
        subjectEntities.addAll ( asList(mainEntity["subject"]) )
    if (mainEntity["instanceOf"]["subject"])
        subjectEntities.addAll ( asList(mainEntity["instanceOf"]["subject"]) )

    def requiredSchemes = [
            "https://id.kb.se/term/sao",
            "https://id.kb.se/term/barn",
    ]

    def modified = false
    subjectEntities.each { subject ->
        if (subject &&
                subject["@type"] &&
                subject["@type"] == "ComplexSubject" &&
                subject["inScheme"] &&
                subject["inScheme"]["@id"] &&
                requiredSchemes.contains(subject["inScheme"]["@id"]) ) {
            modified |= recreatePrefLabel(subject, data.whelk)
        }
    }

    if (modified)
        data.scheduleSave()
}

boolean recreatePrefLabel(Map complexSubject, Whelk whelk) {
    String oldPrefLabel = complexSubject["prefLabel"]
    String newPrefLabel = null

    for (Object term : complexSubject["termComponentList"]) {

        // Determine the correct prefLabel for this term.
        String termPrefLabel = null
        if (term["@id"]) {
            Document termDoc = whelk.storage.getDocumentByIri(term["@id"])
            termPrefLabel = asList(termDoc.data["@graph"][1]["prefLabel"])[0]
        } else if (term["prefLabel"]) {
            termPrefLabel = asList(term["prefLabel"])[0]
        }

        if (termPrefLabel != null) {
            if (newPrefLabel == null)
                newPrefLabel = termPrefLabel
            else
                newPrefLabel += "--" + termPrefLabel
        } else {
            System.err.println("Could not determine the proper prefLabel for: " + term)
            return false
        }
    }

    if (oldPrefLabel != null && oldPrefLabel != "" && !newPrefLabel.equalsIgnoreCase(oldPrefLabel)) {
        complexSubject["prefLabel"] = newPrefLabel

        if (complexSubject["sameAs"]) {
            for (Object sameAs : complexSubject["sameAs"]) {
                String sameAsUri = sameAs["@id"]
                int splitAt = sameAsUri.lastIndexOf("/")+1
                String newSameAsUri = sameAsUri.substring(0, splitAt) + newPrefLabel
                sameAs["@id"] = newSameAsUri
            }
        }

        return true
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
