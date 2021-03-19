String where = """
    collection = 'bib' 
    AND (data#>>'{@graph,1,marc:hasImmediateSourceOfAcquisitionNote}' LIKE '%\"marc:isPrivate\"%'
    OR data#>>'{@graph,1,hasNote}' LIKE '%\"marc:isPrivate\"%')
"""

selectBySqlWhere(where) { data ->
    boolean modified

    if (data.graph[1].containsKey("marc:hasImmediateSourceOfAcquisitionNote")) {
        asList(data.graph[1]["marc:hasImmediateSourceOfAcquisitionNote"]).each { note ->
            modified |= note.removeAll{ key, value ->
                key == "marc:isPrivate" && value == false
            }
        }
    }

    if (data.graph[1].containsKey("hasNote")) {
        asList(data.graph[1]["hasNote"]).each { note ->
            modified |= note.removeAll{ key, value ->
                key == "marc:isPrivate" && value == false
            }
        }
    }

    if (modified) {
        data.scheduleSave()
    }
}

List asList(Object o) {
    return o instanceof List ? o : [o]
}