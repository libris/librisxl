import whelk.Whelk
import whelk.JsonLd
import whelk.converter.TrigToJsonLdParser

List<Map> loadDescriptions(Whelk whelk, String rdfSourcePath) {
    Map data = new File(rdfSourcePath).withInputStream { TrigToJsonLdParser.parse(it) }
    contextDocData = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    return TrigToJsonLdParser.compact(data, contextDocData)[GRAPH]
}

boolean update(JsonLd jsonld, Map mainEntity, Map desc, replaceSingle=true) {
    assert mainEntity[ID] == desc[ID]
    for (def key in desc.keySet()) {
        def value = desc[key]
        List values = value instanceof List ? value : [value]

        // Expand prefix names to full URIs
        for (def v in values) {
            if (v instanceof Map && ID in v) {
                v[ID] = jsonld.expand(v[ID])
            }
        }

        if (key !in mainEntity) mainEntity[key] = []
        def existing = mainEntity[key]

        // Add to set
        if (jsonld.isSetContainer(key)) {
            List allExisting = existing instanceof List ? existing : [existing]
            Set<String> existingLinks = allExisting.findResults {
                it instanceof Map ? it[ID] : null
            } as Set
            mainEntity[key] += values.findAll {
                it instanceof Map ? it[ID] !in existingLinks : it !in existing
            }
        // Overwrite language value
        } else if (jsonld.isLangContainer(key) &&
                   value instanceof Map &&
                   existing instanceof Map) {
            value.each { lang, string ->
                existing[lang] = string
            }
        // Overwrite non-set value
        } else {
            mainEntity[key] = values[0]
        }
    }

    return true
}

Map descriptionMap = [:]

String rdfPatchFile = System.getProperty("rdfpatch")
var graph = loadDescriptions(getWhelk(), rdfPatchFile)
for (var desc : graph) {
    id = desc[ID]
    descriptionMap[id] = desc
}

Set<String> createdIds = new HashSet()

selectByIds(descriptionMap.keySet()) { dataItem ->
    def mainEntity = dataItem.graph[1]
    def mainId = mainEntity[ID]
    Map desc = descriptionMap[mainId]
    if (update(dataItem.whelk.jsonld, mainEntity, desc)) {
        createdIds << mainId
        dataItem.scheduleSave()
    }
}

List<Map> newDocs = descriptionMap.values().findResults {
    if (it[ID] !in createdIds) {
        println "Unxpected NEW: ${it[ID]}"
    }
}
