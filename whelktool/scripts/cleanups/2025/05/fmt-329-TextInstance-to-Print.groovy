String where = "collection='bib' and data#>>'{@graph,1,@type}' = 'TextInstance'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    def instance = data.graph[1]

    if (instance["@type"] == "TextInstance") {
        instance["@type"] = "Print"
        changed = true
    }

    if (changed) {
        data.scheduleSave()
    }
}