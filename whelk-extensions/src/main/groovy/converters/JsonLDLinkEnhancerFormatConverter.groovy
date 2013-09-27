package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.ElasticQuery

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDLinkEnhancerFormatConverter extends BasicFormatConverter implements WhelkAware {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def whelk

    public void setWhelk(Whelk whelk) {
        this.whelk = whelk
    }

    Document doConvert(Document doc) {
        def changedData = false
        def authDataJson

        def json = mapper.readValue(doc.dataAsString, Map)

        for (link in doc.links) {
            if (link.type == "auth") {
                def authDoc = whelk.get(link.identifier)
                if (authDoc) {
                    authDataJson = mapper.readValue(authDoc.dataAsString, Map)
                    switch (authDataJson["about"]["@type"] as String) {
                        case "Person":
                            if (json["about"]["instanceOf"].containsKey("creator")) {
                                def creatorProp = json["about"]["instanceOf"]["creator"]
                                if (creatorProp instanceof List) {
                                    creatorProp.eachWithIndex { c, ind ->
                                        if (c["@type"] == "Person" && c["controlledLabel"] == authDataJson["about"]["controlledLabel"]) {
                                            json["about"]["instanceOf"]["creator"][ind]["@id"] = authDataJson["about"]["@id"]
                                            changedData = true
                                        }
                                    }
                                } else if (creatorProp instanceof Map) {
                                    if (creatorProp["@type"] == "Person" && creatorProp["controlledLabel"] == authDataJson["about"]["controlledLabel"]) {
                                        json["about"]["instanceOf"]["creator"]["@id"] = authDataJson["about"]["@id"]
                                        changedData = true
                                    }
                                }
                            }
                            if (json["about"]["instanceOf"].containsKey("contributorList")) {
                                def contributorProp = json["about"]["instanceOf"]["contributorList"]
                                if (contributorProp instanceof List) {
                                    contributorProp.eachWithIndex { c, ind ->
                                        if (c["@type"] == "Person" && c.get("label", null) == authDataJson["about"]["controlledLabel"]) {
                                            json["about"]["instanceOf"]["contributorList"][ind]["@id"] = authDataJson["about"]["@id"]
                                            changedData = true
                                        }
                                    }
                                }
                            }
                            break
                        case "Concept":
                            if (json["about"]["instanceOf"].containsKey("subject")) {
                                json["about"]["instanceOf"]["subject"].eachWithIndex { subj, index ->
                                    if (subj["@type"] == "Concept") {
                                        subj["broader"].eachWithIndex { it, i ->
                                            if (it.get("prefLabel", null) && it["prefLabel"] == authDataJson["about"]["prefLabel"]) {
                                                json["about"]["instanceOf"]["subject"][index]["broader"][i]["@id"] = authDataJson["about"]["sameAs"]["@id"]
                                                changedData = true
                                            }
                                        }
                                    }
                                }
                            }
                    }
                }
            }

        }
        if (changedData) {
             return doc.withData(mapper.writeValueAsString(json))
        }
        return doc
    }
}



