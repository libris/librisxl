selectBySqlWhere("collection = 'bib' and deleted = false and modified > '20231213' and data#>'{@graph,1,instanceOf,0}' is not null") {
    def instance = it.graph[1]
    assert instance['instanceOf'].size() == 1
    instance['instanceOf'] = instance['instanceOf'][0]
    it.scheduleSave()
}
