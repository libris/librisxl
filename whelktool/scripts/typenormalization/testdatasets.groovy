Closure normalizeTypes = script("${System.properties['typenormalization'] ?: 'simple-types-algorithm'}.groovy")

var loadWorkItem = { String workId, Closure process -> selectByIds([workId], process) }

selectBySqlWhere("""
    data#>'{@graph,0,inDataset}' @> '[{"@id": "https://libris.kb.se/dataset/instances"}]'::jsonb
""") {
  normalizeTypes(it, loadWorkItem)
}
