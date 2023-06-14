String where = "collection = 'bib' and data#>>'{@graph,1,instanceOf,contribution}' like '%marc:uri%'"

selectBySqlWhere(where) { data ->
    boolean changed = false
    List contributions = asList(data.graph[1].instanceOf.contribution)
    contributions.each { contrib ->
        if (contrib.agent) {
            Map agent = contrib.agent
            if (agent["marc:uri"] != null) {

                def uri = agent["marc:uri"]
                if (uri instanceof List) {
                    uri = uri[0]
                }

                //System.err.println("Replacing:\n\t" + agent + " with link:\n\t" + uri)
                agent.clear()
                agent["@id"] = uri
                changed = true
            }
        }
    }

    if (changed) {
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
