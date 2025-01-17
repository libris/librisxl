Closure normalizeTypes = script('algorithm.groovy')

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

selectByCollection('bib') {
  incrementStats('1.1 Instance type - before update', it.graph[1]['@type'])
  incrementStats('2.1 Work type - before update', it.graph[1]['instanceOf']['@type'])

  normalizeTypes(it, loadWorkItem)
  
  incrementStats('1.2 Instance typ - After update', it.graph[1]['@type'])
  incrementStats('2.2 Work type - After update', it.graph[1]['instanceOf']['@type'])
}
