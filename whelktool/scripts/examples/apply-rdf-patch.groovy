import whelk.JsonLd
import whelk.converter.TrigToJsonLdParser
import whelk.datatool.DocumentItem

List<Map> loadPatchGraph(String rdfPatchFile) {
    Map data = new File(rdfPatchFile).withInputStream { TrigToJsonLdParser.parse(it) }
    whelk = getWhelk()
    contextDocData = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    return TrigToJsonLdParser.compact(data, contextDocData)[GRAPH]
}

boolean update(JsonLd jsonld, Map mainEntity, Map desc, replaceSingle=true) {
    assert mainEntity[ID] == desc[ID]
    for (def key in desc.keySet()) {
        def value = desc[key]
        List values = value instanceof List ? value : [value]
        for (def v in values) {
            if (v instanceof Map && ID in v) {
                v[ID] = jsonld.expand(v[ID])
            }
        }

        if (jsonld.isSetContainer(key)) {
            if (key !in mainEntity) mainEntity[key] = []
            mainEntity[key] += values
        } else {
            mainEntity[key] = values[0]
        }
    }

    return true
}

Map descriptionMap = [:]
List thingIds = []

String rdfPatchFile = System.getProperty("rdfpatch")
var graph = loadPatchGraph(rdfPatchFile)
for (desc in graph) {
    id = desc[ID]
    descriptionMap[id] = desc
    thingIds << id
}

selectByIds(thingIds) { dataItem ->
    def mainEntity = dataItem.graph[1]
    def mainId = mainEntity[ID]
    Map desc = descriptionMap[mainId]
    if (update(dataItem.whelk.jsonld, mainEntity, desc)) {
        dataItem.scheduleSave()
    }
}
