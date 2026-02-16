Closure normalizeTypes = script("${System.properties['typenormalization'] ?: 'simple-types-algorithm'}.groovy")

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

var examplesFile = new File(scriptDir, 'examples.txt')
var ids = examplesFile.iterator().findResults {
  (it =~ /^[^#]*<([^>]+?(?:([^\/#]+)#it)?)>/).findResult { m, iri, xlid -> xlid }
}

selectByIds(ids) {
  normalizeTypes(it, loadWorkItem)
}
