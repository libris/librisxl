String where = """
    collection = 'bib' 
    AND data#>>'{@graph,1,marc:hasImmediateSourceOfAcquisitionNote}' LIKE '%\"marc:isPrivate\"%'
"""

selectBySqlWhere(where) { data ->
    boolean modified

    asList(data.graph[1]["marc:hasImmediateSourceOfAcquisitionNote"]).each { note ->
        modified |= note.removeAll{ key, value ->
            key == "marc:isPrivate" && value == false
        }
    }

    if (modified) {
        data.scheduleSave()
    }
}

List asList(Object o) {
    return o instanceof List ? o : [o]
}