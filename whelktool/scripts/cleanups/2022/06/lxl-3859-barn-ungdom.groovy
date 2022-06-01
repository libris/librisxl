/*
I bibliografiska beskrivningar som har lokala formunderindelningen "barn- och ungdomslitteratur" i egenskapen Ämne ska
den/dessa raderas och den bibliografiska beskrivningen ska kompletteras med genre/formtermen Barn- och ungdomslitteratur
från systemet barngf i de fall en sådan term inte redan är inlagd.
 */

String where = """
        collection = 'bib' AND deleted = false AND
        data#>>'{@graph,1,instanceOf,subject}' LIKE '%barn- och ungdomslitteratur%'
"""

selectBySqlWhere(where) { data ->
    boolean changed = false
    def instance = data.graph[1]

    Map subjectToPreserve = null

    instance.instanceOf?.subject?.removeAll { subject ->

        if (subject["@type"] == "ComplexSubject" && subject.inScheme?.containsKey("@id") && subject.inScheme["@id"] == "https://id.kb.se/term/barn") {
            if (subject.termComponentList.removeAll { termComponent ->
                return termComponent["@type"] == "GenreSubdivision" && termComponent.prefLabel == "barn- och ungdomslitteratur"
                }) {
                changed = true
            }

            // Only the main term still remaining? - Remove the whole subject
            if (subject.termComponentList.size() == 1) {
                subjectToPreserve = subject.termComponentList[0]
                return true
            }
        }

        return false
    }

    if (subjectToPreserve != null) {
        if (! instance.instanceOf.subject.contains(subjectToPreserve))
            instance.instanceOf.subject.add(subjectToPreserve)
    }

    if (changed) {
        Map gf = ["@id": "https://id.kb.se/term/barngf/Barn-%20och%20ungdomslitteratur"]
        if (instance.instanceOf.genreForm == null)
            instance.instanceOf["genreForm"] = []
        if (! instance.instanceOf.genreForm.contains(gf))
            instance.instanceOf.genreForm.add(gf)
        //System.err.println("result:\n" + data.doc.getDataAsString() + "\n\n")
        data.scheduleSave()
    }
}
