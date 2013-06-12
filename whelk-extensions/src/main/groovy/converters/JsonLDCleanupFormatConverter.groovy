package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupFormatConverter extends BasicFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    
    Document doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)
        def fieldLookUp = [
            "titleValue" : ["title" : [":", "/"]],
            "providerDate" : ["publication" : [",", ";"]],
            "providerName" : ["publication" : [",", ":", ";"]],
            "place" : ["publication" : [",", ":"]],
            "place" : ["manufacture" : [":", ";"]],
            "extent" : ["extent" : ["+", ":"]],
            "otherPhysicalDetails" : ["otherPhysicalDetails" : [";", "+"]],
            "dimensions" : ["dimensions" : [";", "+"]],
            "edition" : ["edition" : ["/", "=", ","]],
            "title" : ["series" : [".", ",", "=", ";"]],
            "part" : ["series" : [".", ",", "=", ";"]],
            "issn" : ["series" : [".", ",", "=", ";"]],
        ]

        fieldLookUp.each { propName, interpunctionMap ->
            interpunctionMap.each { entityName, interpunctionList ->
                def entity = json.about.get((entityName), null)
                if (entity) {
                    log.debug("Entity ${entity}")
                    if (entity instanceof String && entity.size() > 1) {
                        interpunctionList.each {
                            if (entity[-1].equals(it)) {
                                json.about[(entityName)] = entity[0..-2].trim()
                            }
                        }
                    } else if (entity instanceof Map) {
                        if (entity.get((propName), null)) {
                            interpunctionList.each {
                                if (entity[(propName)][-1].equals(it)) {
                                    json.about[(entityName)][(propName)] = entity[(propName)][0..-2].trim()
                                }
                            }
                        }
                    } else if (entity instanceof List) {
                        entity.eachWithIndex { entIt, index ->
                            if (entIt.get((propName), null)) {
                                if (entIt[(propName)] instanceof String && entIt[(propName)].size() > 1) {
                                    interpunctionList.each { interpIt ->
                                        if (entIt[(propName)][-1].equals(interpIt)) {
                                            json.about[(entityName)][index][(propName)] = entIt[(propName)][0..-2].trim()
                                        }
                                    }
                                } else if (entIt[(propName)] instanceof Map) {
                                    interpunctionList.each { interpunctionItem ->
                                        if (entIt[(propName)].get("label", null) && entIt[(propName)]["label"].size() > 1) {
                                            log.debug("last " + entIt[(propName)]["label"][-1] + " interpIt ${interpunctionItem}")
                                            if (entIt[(propName)]["label"][-1].equals(interpunctionItem)) {
                                                json.about[(entityName)][index][(propName)]["label"] = entIt[(propName)]["label"][0..-2].trim()
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    doc = doc.withData(mapper.writeValueAsBytes(json))
    return doc
}
}
