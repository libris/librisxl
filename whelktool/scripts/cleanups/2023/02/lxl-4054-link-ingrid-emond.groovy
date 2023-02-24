String where = "data#>'{@graph,1,instanceOf,contribution}' @> '[{\"agent\":{\"familyName\":\"Emond\",\"givenName\":\"Ingrid\"}}]'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    List contributions = asList(data.graph[1].instanceOf.contribution)
    for (Map contribution : contributions) {
        if (contribution?.agent?.givenName == "Ingrid" && contribution?.agent?.familyName == "Emond" && contribution?.agent.toString().contains("1925")) {
            contribution.agent = ["@id" : baseUri.resolve("/gdsvvw30307mgbk#it").toString()]
            //System.err.println("New agent: " + contribution.agent)
            changed = true
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
