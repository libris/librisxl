Closure normalizeTypes = script('algorithm.groovy')

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

selectByCollection('bib') {
  normalizeTypes(it, loadWorkItem)
}
