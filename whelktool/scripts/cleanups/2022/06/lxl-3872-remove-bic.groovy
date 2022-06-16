String where = """
        collection = 'bib' AND deleted = false AND
        data#>>'{@graph,1,instanceOf,subject}' LIKE '%bicssc%'
"""

selectBySqlWhere(where) { data ->
    boolean modified = false

    data.graph[1].instanceOf?.subject?.removeAll { subject ->
        for (Object o : asList(subject.inScheme)) {
            Map scheme = (Map) o
            if (scheme["@id"] == "https://id.kb.se/term/bicssc" || scheme["code"] == "bicssc") {
                modified = true
                return true
            }
        }
        return false
    }

    if (asList(data.graph[1].instanceOf?.subject).size() == 0)
        data.graph[1].instanceOf.remove("subject")

    if (modified) {
        //System.err.println("result:\n" + data.doc.getDataAsString() + "\n\n")
        data.scheduleSave()
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
