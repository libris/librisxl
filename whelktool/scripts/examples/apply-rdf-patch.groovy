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
        modified |= expandLink(jsonld, newValue)

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

            for (def v : newValues) {
                modified |= expandLink(jsonld, v)
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
        } else if (mainEntity[key] != newValue) {
            mainEntity[key] = newValue
            modified = true
        }
    }

    return modified
}

boolean expandLink(JsonLd jsonld, o) {
    if (o instanceof Map && ID in o) {
        def expandedId = jsonld.expand(o[ID])
        if (expandedId != o[ID]) {
            o[ID] = expandedId
            return true
        }
    }
    return false
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

Set<String> existingIds = new HashSet()

selectByIds(descriptionMap.keySet()) { dataItem ->
    def mainEntity = dataItem.graph[1]
    def mainId = mainEntity[ID]
    Map desc = descriptionMap[mainId]
    existingIds << mainId
    if (update(dataItem.whelk.jsonld, mainEntity, desc, deleteShape)) {
        dataItem.scheduleSave()
    }
}

List<Map> newDocs = descriptionMap.values().findResults {
    // Expand prefixed names in links
    var jsonld = getWhelk().jsonld
    for (def key : it.keySet()) {
        def value = it[key]
        if (value instanceof List) {
            for (def v : value) {
                expandLink(jsonld, v)
            }
        } else {
            expandLink(jsonld, value)
        }
    }

    if (it[ID] !in existingIds) {
        create( [ "@graph": [
            [
                "@id": "TEMPID",
                "@type": "Record",
                "mainEntity" : ["@id": it[ID]]
            ],
            it
        ]])
    }
}

selectFromIterable(newDocs, { newItem ->
    newItem.scheduleSave()
})
