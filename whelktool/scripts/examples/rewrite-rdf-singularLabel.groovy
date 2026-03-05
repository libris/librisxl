import whelk.Whelk
import whelk.JsonLd
import whelk.converter.TrigToJsonLdParser

import java.util.concurrent.ConcurrentHashMap

List<Map> loadDescriptions(Whelk whelk, String rdfSourcePath) {
    Map data = new File(rdfSourcePath).withInputStream { TrigToJsonLdParser.parse(it) }
    contextDocData = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    def results = TrigToJsonLdParser.compact(data, contextDocData)

    var graphButNotNamed = GRAPH in results && ID !in results
    return graphButNotNamed ? results[GRAPH] : (results instanceof List ? results : [results])
}


boolean fixSingularLabel(JsonLd jsonld, Map mainEntity, Map desc) {
    var modified = false

    var BAD_PROP = 'singularLabel'
    var GOOD_PROP = 'singularLabelByLang'

    assert mainEntity[ID] == desc[ID]

    if (BAD_PROP in mainEntity) {
      if (desc[GOOD_PROP] instanceof Map && '@value' !in desc.get(GOOD_PROP)) {
        mainEntity.remove(BAD_PROP)
        mainEntity[GOOD_PROP] = desc[GOOD_PROP]
        modified = true
      }
    }

    return modified
}

Map descriptionMap = [:]

String rdfDataFile = System.getProperty("rdfdata")

var graph = loadDescriptions(getWhelk(), rdfDataFile)
for (var desc : graph) {
    id = desc[ID]
    if (!id) {
        continue
    }
    descriptionMap[id] = desc
}

selectByIds(descriptionMap.keySet()) { dataItem ->
    def mainEntity = dataItem.graph[1]
    def mainId = mainEntity[ID]

    Map desc = descriptionMap[mainId]
    if (desc && fixSingularLabel(dataItem.whelk.jsonld, mainEntity, desc)) {
        dataItem.scheduleSave()
    }
}
