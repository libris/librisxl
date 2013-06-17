package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupFormatConverter extends BasicFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def fieldLookUp = [
            "titleValue_title" : [":", "/"],
            "subtitle_title" : ["/"],
            "titleValue_titleVariation" : [":", "/"],
            "providerDate_publication" : [",", ";"],
            "providerName_publication" : [",", ":", ";"],
            "place_publication" : [",", ":"],
            "place_manufacture" : [":", ";"],
            "extent" : ["+", ":"],
            "otherPhysicalDetails" : [";", "+"],
            "dimensions" : [":", ";", "+"],
            "edition" : ["/", "=", ","],
            "title" : [".", ",", "=", ";"],
            "part" : [".", ",", "=", ";"],
            "issn" : [".", ",", "=", ";"],
    ]

    Document doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)
        def result = [:]

        fieldLookUp.each { entityProp, interpunctionList ->
            def entityKey
            def propertyKey = entityProp.split("_").getAt(0)
            try {
                entityKey = entityProp.split("_").getAt(1)
            } catch (Exception e) {
                entityKey = null
            }
            log.debug("EntityKey ${entityKey} property ${propertyKey}")
            def entity = (entityKey ? json.about?.get(entityKey, null) : json.get("about", null))
            def prop
            if (entity && entity instanceof Map) {
                prop = entity.get(propertyKey, null)
                result = cleanProperty(prop, entityProp, entityKey, propertyKey, json, -1)
            } else if (entity instanceof List) {
                entity.eachWithIndex { entIt, index ->
                    prop = entIt.get(propertyKey, null)
                    result = cleanProperty(prop, entityProp, json, entityKey, propertyKey, index)
                }
            }
        }

        doc = doc.withData(mapper.writeValueAsBytes(result))
        return doc
    }

    //TODO: bara indexera datum

    Map cleanProperty(def theJson, def prop, def entityProp, def entityKey, def propKey, def index) {
        if (prop && prop instanceof String && prop.size() > 1) {
            def interpunctionList = fieldLookUp[entityProp]
            log.debug("interpunctionlist type " + interpunctionList.getClass())
            for (it in interpunctionList) {
                //log.debug("Prop type " + prop.getClass() + " it class " + it.getClass() + ${entityKey} + " " + ${propKey})
                if (prop instanceof String && prop.size() > 1 && prop[-1].equals(it)) {
                    if (entityKey) {
                        theJson.about[entityKey][propKey] = prop[0..-2].trim()
                    } else {
                        theJson.about[propKey] = prop[0..-2].trim()
                    }
                }
            }
        } else if (prop instanceof Map) {
            interpunctionList.each {
                if (prop.get("label", null) && prop["label"].size() > 1) {
                    if (prop["label"][-1].equals(it)) {
                        theJson.about[entityKey][index][propKey]["label"] = prop["label"][0..-2].trim()
                    }
                }
            }
        }
    }


}
