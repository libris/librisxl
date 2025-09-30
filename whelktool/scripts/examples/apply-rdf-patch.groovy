import whelk.Whelk
import whelk.JsonLd
import whelk.converter.TrigToJsonLdParser

List<Map> loadDescriptions(Whelk whelk, String rdfSourcePath) {
    Map data = new File(rdfSourcePath).withInputStream { TrigToJsonLdParser.parse(it) }
    contextDocData = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    return TrigToJsonLdParser.compact(data, contextDocData)[GRAPH]
}

boolean update(JsonLd jsonld, Map mainEntity, Map desc, deleteShape=null, replaceSingle=true) {
    var modified = false

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

        if (key !in mainEntity) {
            mainEntity[key] = []
            modified = true
        }
        def existing = mainEntity[key]

        // Add to set
        if (jsonld.isSetContainer(key)) {
            List hasValues = existing instanceof List ? existing : [existing]
            Set<String> existingLinks = hasValues.findResults {
                it instanceof Map ? it[ID] : null
            } as Set
            int hadSize = hasValues.size()
            hasValues += values.findAll {
                it instanceof Map ? it[ID] !in existingLinks : it !in hasValues
            }
            if (hasValues.size() > hadSize) {
                modified = true
            }
            mainEntity[key] = hasValues
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

    return modified
}

Map descriptionMap = [:]

String rdfDataFile = System.getProperty("rdfdata")
var graph = loadDescriptions(getWhelk(), rdfDataFile)
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
        create( [ "@graph": [
            [
                "@id": "TEMPID",
                "mainEntity" : ["@id": it[ID]]
            ],
            it
        ]])
    }
}

selectFromIterable(newDocs, { newItem ->
    newItem.scheduleSave()
})
