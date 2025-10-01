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
        def newValue = desc[key]
        // Expand prefix name to full URI
        if (newValue instanceof Map && ID in newValue) {
            newValue[ID] = jsonld.expand(newValue[ID])
            modified = true
        }

        if (key !in mainEntity) {
            mainEntity[key] = []
            modified = true
        }
        def hasValue = mainEntity[key]

        // Add to set
        if (jsonld.isSetContainer(key)) {
            List hasValues = hasValue instanceof List ? hasValue : [hasValue]
            int hadSize = hasValues.size()

            hasValues = hasValues.findAll {
                it !instanceof Map && ID !in it || it[ID] == jsonld.expand(it[ID])
            }

            Set<String> existingLinks = hasValues.findResults {
                it instanceof Map && ID in it ? jsonld.expand(it[ID]) : null
            } as Set

            List newValues = newValue instanceof List ? newValue : [newValue]

            // Expand prefix names to full URIs
            for (def v : newValues) {
                if (v instanceof Map && ID in v) {
                    v[ID] = jsonld.expand(v[ID])
                    modified = true
                }
            }

            newValues = newValues.findAll {
                (it instanceof Map && it[ID] !in existingLinks) || it !in hasValues
            }

            if (newValues.size() > hadSize) {
                modified = true
            }

            mainEntity[key] = hasValues + newValues
        // Overwrite language value
        } else if (jsonld.isLangContainer(key) &&
                   newValue instanceof Map &&
                   hasValue instanceof Map) {
            newValue.each { lang, string ->
                hasValue[lang] = string
            }
        // Overwrite non-set value
        } else {
            mainEntity[key] = newValue
        }
    }

    return modified
}

Map descriptionMap = [:]
Map deleteShape = null

String rdfDataFile = System.getProperty("rdfdata")
var graph = loadDescriptions(getWhelk(), rdfDataFile)
for (var desc : graph) {
    id = desc[ID]
    descriptionMap[id] = desc
    if (desc[TYPE] == 'DeleteShape') {
        deleteShape = desc
    }
}

Set<String> createdIds = new HashSet()

selectByIds(descriptionMap.keySet()) { dataItem ->
    def mainEntity = dataItem.graph[1]
    def mainId = mainEntity[ID]
    Map desc = descriptionMap[mainId]
    if (update(dataItem.whelk.jsonld, mainEntity, desc, deleteShape)) {
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
