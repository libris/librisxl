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
                def authDoc = whelk.get(new URI(link.identifier))
                if (authDoc == null) {
                    continue
                }
                authDataJson = mapper.readValue(authDoc.dataAsString, Map)
                def work = json["about"]["instanceOf"]
                def authItem = authDataJson["about"]
                switch (authItem["@type"] as String) {
                    case "Person":
                        if (work.containsKey("creator")) {
                            def creator = work["creator"]
                            if (creator instanceof List) {
                                creator.each {
                                    if (updatePersonId(it, authItem))
                                        changedData = true
                                }
                            } else if (creator instanceof Map) {
                                if (updatePersonId(creator, authItem))
                                    changedData = true
                            }
                        }
                        if (work.containsKey("contributorList")) {
                            def contributors = work["contributorList"]
                            if (contributors instanceof List) {
                                contributors.each {
                                    if (updatePersonId(it, authItem, "label"))
                                        changedData = true
                                }
                            }
                        }
                        break
                    case "Concept":
                        def sameAs = "/resource" + authDataJson["@id"]
                        if (work.containsKey("subject")) {
                            def concepts = work["subject"]
                            concepts.each {
                                if (it["@type"] == "Concept") {
                                    it["broader"].each { broader ->
                                        if (updateConceptId(broader, authItem, sameAs))
                                            changedData = true
                                    }
                                }
                            }
                        }
                        if (work.containsKey("class")) {
                            def concepts = work["class"]
                            concepts.each {
                                if (it["@type"] == "Concept") {
                                    if (it.get("@id", null) && it["@id"] == authItem["@id"]) {
                                        it["sameAs"] = ["@id": sameAs]
                                        changedData = true
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

    boolean updatePersonId(item, authItem, label="controlledLabel") {
        if (item["@type"] == "Person" && item[label] == authItem["controlledLabel"]) {
            item["@id"] = authItem["@id"]
            return true
        }
        return false
    }

    boolean updateConceptId(item, authItem, sameAs) {
        if (item.get("prefLabel", null) && item["prefLabel"] == authItem["prefLabel"]) {
            item["sameAs"] = ["@id": sameAs]
            return true
        }
        return false
    }

}
