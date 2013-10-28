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
                                    if (updatePersonId(it, authItem))
                                        changedData = true
                                }
                            }
                        }
                        break
                    case "Concept":
                        if (work.containsKey("subject")) {
                            def concepts = work["subject"]
                            concepts.each {
                                if (it["@type"] == "Concept") {
                                    if (updateConceptId(it, authItem))
                                        changedData = true
                                    it["broader"].each { broader ->
                                        if (updateConceptId(broader, authItem))
                                            changedData = true
                                    }
                                }
                            }
                        }
                        if (work.containsKey("class")) {
                            def concepts = work["class"]
                            concepts.each {
                                if (it["@type"] == "Concept") {
                                    if (updateConceptId(it, authItem))
                                        changedData = true
                                }
                            }
                        }
                        break
                    case "Work":
                        if (work.containsKey("subject")) {
                            def concepts = work["subject"]
                            concepts.each {
                                if (it["@type"] == "Work") {
                                    if (updateWorkId(it, authItem))
                                        changedData = true
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

    boolean updatePersonId(item, authItem) {
        if (item["@type"] == "Person" && item["controlledLabel"] == authItem["controlledLabel"]) {
            item["@id"] = authItem["@id"]
            return true
        }
        return false
    }

    boolean updateConceptId(item, authItem) {
        def authItemSameAs = authItem.get("sameAs")?.get("@id")
        if (item["@id"] && item["@id"] == authItemSameAs) {
            item["sameAs"] = ["@id": item["@id"]]
            item["@id"] = authItem["@id"]
            return true
        }
        def same = item.get("sameAs")?.get("@id") == authItemSameAs
        if (same || (item["prefLabel"] && item["prefLabel"] == authItem["prefLabel"])) {
            item["@id"] = authItem["@id"]
            return true
        }
        return false
    }

    boolean updateWorkId(item, authItem) {
        if (item["@type"] == "Work" && item["uniformTitle"] == authItem["uniformTitle"]) {
            def creator = item.creator
            if (creator instanceof List)
                creator = creator[0]
            def authCreator = authItem.creator
            if (authCreator instanceof List)
                authCreator = authCreator[0]
            if (
                    (!creator && !authCreator) ||
                    ((creator && authCreator) &&
                     (creator["@type"] == "Person" && authCreator["@type"] == "Person" &&
                      creator["controlledLabel"] == authCreator["controlledLabel"]))
               ) {
                item["@id"] = authItem["@id"]
                return true
            }
        }
        return false
    }

}
