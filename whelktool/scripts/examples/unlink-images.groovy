String where = "deleted = false and collection = 'bib' and data#>'{@graph,1,image}' is not null"

selectBySqlWhere(where) { data ->
    boolean changed = data.graph[1].remove("image")
    if (changed) {
        data.scheduleSave()
    }
}