Closure normalizeTypes = script('algorithm.groovy')

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

var examplesFile = new File(scriptDir, 'lisas-examples.txt')
var ids = examplesFile.iterator().findResults {
  (it =~ /^[^#]*<([^>]+?(?:([^\/#]+)#it)?)>/).findResult { m, iri, xlid -> xlid }
}

selectByIds(ids) {
  def mapped_fields = normalizeTypes(it, loadWorkItem)
  println "In the main fucntion printing $mapped_fields"

  incrementStats('1.1 Instance: Mapped intersection', mapped_fields)
  incrementStats('1.2 Instance typ - After update', it.graph[1]['@type'])
  incrementStats('2.2 Work type - After update', it.graph[1]['instanceOf']['@type'])
}
