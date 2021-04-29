String where = """
    data#>'{@graph,1,instanceOf,genreForm}' @> '[{"@id":"https://id.kb.se/term/sao/Jaktskildringar"}]'
    AND collection = 'bib'
"""

selectBySqlWhere(where) { data ->
    asList(data.graph[1].instanceOf.genreForm).each { gf ->
        if (gf["@id"] == "https://id.kb.se/term/sao/Jaktskildringar") {
            gf["@id"] = "https://id.kb.se/term/saogf/Jaktskildringar"
            data.scheduleSave()
        }
    }
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
