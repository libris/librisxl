import whelk.Whelk
import whelk.JsonLd
import whelk.converter.TrigToJsonLdParser

List<Map> loadDescriptions(Whelk whelk, String rdfSourcePath) {
    Map data = new File(rdfSourcePath).withInputStream { TrigToJsonLdParser.parse(it) }
    contextDocData = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    def results = TrigToJsonLdParser.compact(data, contextDocData)

    var graphButNotNamed = GRAPH in results && ID !in results
    return graphButNotNamed ? results[GRAPH] : (results instanceof List ? results : [results])
}

boolean update(JsonLd jsonld, Map mainEntity, Map desc, replaceSingle=true) {
    var modified = false

    assert mainEntity[ID] == desc[ID]
    for (def key in desc.keySet()) {
        def newValue = desc[key]
        expandLink(jsonld, newValue)

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
                expandLink(jsonld, v)
            }

            newValues = newValues.findAll {
                (it instanceof Map && it[ID] !in existingLinks) || it !in hasValues
            }

            if (newValues.size() > 0) {
                modified = true
            }

            mainEntity[key] = hasValues + newValues
        // Overwrite language value
        } else if (key in jsonld.langContainerAliasInverted &&
                   newValue instanceof Map &&
                   hasValue instanceof Map) {
            newValue.each { lang, string ->
                if (hasValue[lang] != string) {
                    hasValue[lang] = string
                    modified = true
                }
            }
        // Overwrite non-set value
        } else if (mainEntity[key] != newValue) {
            mainEntity[key] = newValue
            modified = true
        }
    }

    return modified
}

boolean delete(JsonLd jsonld, Map mainEntity, Map dropDesc) {
    var modified = false
    for (def key in dropDesc.keySet()) {
        if (key == ID) {
            continue
        }

        def dropValue = dropDesc[key]

        var dropValues = new HashSet()
        if (dropValue instanceof List) {
            for (def v in dropValue) {
                expandLink(jsonld, v)
            }
            dropValues += dropValue
        } else {
            expandLink(jsonld, dropValue)
            dropValues << dropValue
        }

        if (key in mainEntity) {
            def hasValue = mainEntity[key]
            if (jsonld.isSetContainer(key) && hasValue instanceof List) {
                List keptValues = hasValue.findAll {
                    return it !instanceof Map || it !in dropValues
                }
                if (keptValues.size() == 0) {
                    mainEntity.remove(key)
                    modified = true
                } else if (keptValues.size() < hasValue.size()) {
                    mainEntity[key] = keptValues
                    modified = true
                }
            } else if (key in jsonld.langContainerAliasInverted &&
                       dropValue instanceof Map &&
                       hasValue instanceof Map) {
                dropValue.each { lang, value ->
                    if (hasValue[lang] == value) {
                        // TODO: if (value instanceof List) handle each in value set...
                        hasValue.remove(lang)
                        modified = true
                    }
                }
            } else {
                if (hasValue == dropValue) {
                    mainEntity.remove(key)
                    modified = true
                }
            }
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
Map deletionMap = [:]

String rdfDataFile = System.getProperty("rdfdata")
String rdfDeletionGraphId = System.getProperty("rdfdelete")

var graph = loadDescriptions(getWhelk(), rdfDataFile)
for (var desc : graph) {
    id = desc[ID]
    if (rdfDeletionGraphId != null && rdfDeletionGraphId == id) {
        for (var delDesc : desc[GRAPH]) {
            if (ID in delDesc) {
                deletionMap[delDesc[ID]] = delDesc
            }
        }
    } else {
        descriptionMap[id] = desc
    }
}

Set<String> existingIds = new HashSet()

selectByIds(descriptionMap.keySet() + deletionMap.keySet()) { dataItem ->
    def mainEntity = dataItem.graph[1]
    def mainId = mainEntity[ID]
    existingIds << mainId

    Map desc = descriptionMap[mainId]
    if (desc && update(dataItem.whelk.jsonld, mainEntity, desc)) {
        dataItem.scheduleSave()
    }

    Map deleteDesc = deletionMap[mainId]
    if (deleteDesc && delete(dataItem.whelk.jsonld, mainEntity, deleteDesc)) {
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
