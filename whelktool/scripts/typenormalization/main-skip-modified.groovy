package typenormalization

Closure normalizeTypes = script("${System.properties['typenormalization'] ?: 'algorithm'}.groovy")

var loadWorkItem = { String workId, Closure process ->
  selectByIds([workId], process)
}

Set<String> ids = new File(scriptDir, "MODIFIED.txt").readLines() as Set<String>

selectByCollection('bib') {
  if (it.shortId in ids) {
    return
  }
  
  normalizeTypes(it, loadWorkItem)
}
