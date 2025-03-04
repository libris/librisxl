Closure normalizeTypes = script("${System.properties['typenormalization'] ?: 'algorithm'}.groovy")

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

selectByCollection('bib') {
  normalizeTypes(it, loadWorkItem)
}
