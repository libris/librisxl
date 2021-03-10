
selectBySqlWhere("collection = 'bib' and data#>'{@graph,1,marc:hasBib976}' is not null") { data ->
  data.graph[1].remove("marc:hasBib976")
  data.scheduleSave()
}
