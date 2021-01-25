String where = "collection = 'bib' AND data#>'{@graph,1,hasComponent}' @> '[{}]'"

selectBySqlWhere(where) { data ->
    List hasComponent = data.graph[1].hasComponent
    hasComponent.removeAll{ !it }
    if (!hasComponent)
        data.graph[1].remove('hasComponent')
    data.scheduleSave()
}
