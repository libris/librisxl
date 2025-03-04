Closure normalizeTypes = script("${System.properties['typenormalization'] ?: 'algorithm'}.groovy")

var loadWorkItem = { String workId, Closure process -> selectByIds([workId], process) }

selectBySqlWhere("""
    data#>'{@graph,0,inDataset}' @> '[{"@id": "https://libris.kb.se/dataset/instances"}]'::jsonb
""") {
  normalizeTypes(it, loadWorkItem)
}
