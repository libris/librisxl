String where = "collection = 'bib' and data#>>'{@graph,1,identifiedBy}' like '%talbok_text%'"

selectBySqlWhere(where) { data ->
    boolean changed = false
    List identifiedBys = asList(data.graph[1].identifiedBy)

    changed = identifiedBys.removeAll { it.value == "talbok_text" }

    if (changed) {
        identifiedBys.each { identifiedBy ->
            if (identifiedBy.typeNote == "MTM medienummer") {
                if (identifiedBy.qualifier == null) {
                    identifiedBy.qualifier = ["talbok_text"]
                } else if (identifiedBy.typeNote instanceof List) {
                    identifiedBy.qualifier.add("talbok_text")
                }
            }
        }

        //System.err.println("Changed identifiedBy to: " + data.graph[1].identifiedBy)
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
