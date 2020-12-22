where = "collection = 'auth' and data#>>'{@graph,0,_marcUncompleted}' LIKE '%\"045\":%'"

selectBySqlWhere(where) { data ->
    List marcUncompleted = data.graph[0]._marcUncompleted
    marcUncompleted.removeAll { it.containsKey('045') }
    if (!marcUncompleted)
        data.graph[0].remove('_marcUncompleted')
    data.scheduleSave()
}
