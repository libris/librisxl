package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def authPoint = ["Person" : "controlledLabel", "Concept" : "prefLabel", "ConceptScheme" : "notation"]
    def entitiesToExtract = ["creator", "contributorList", "subject", "inScheme"]

    List<IndexDocument> doConvert(Document doc) {

        List<IndexDocument> doclist = [new IndexDocument(doc)]
        log.info("doc data to be deserialized: ${doc.dataAsString}")
        def json = mapper.readValue(doc.dataAsString, Map)

            if (json?.get("@type", null)) {
                log.debug("Record has a @type. Adding to entity recordtype.")
                doclist << createEntityDoc(json, doc.identifier, 3, false)
            }

            if (json?.about?.get("@type", null)) {
                log.debug("Found authority entity. Extracting " + json.about.get("@type"))
                if (json.about.get("@type") == "Person") {
                    doclist << createEntityDoc(json.about, doc.identifier, 10, false)
                } else {
                    doclist << createEntityDoc(json.about, doc.identifier, 0, false)
                }
            }

            json?.about?.each { k, entities ->
                if (entitiesToExtract.contains(k)) {
                    log.debug("Extracting $k")
                    doclist += extractEntities(k, entities, doc.identifier)
                }
            }

            json?.about?.instanceOf?.each { k, entities ->
                if (entitiesToExtract.contains(k)) {
                    log.debug("Extracting $k")
                    doclist += extractEntities(k, entities, doc.identifier)
                }
            }

            json?.about?.subject?.each {
                it.each { k , entities ->
                    if (entitiesToExtract.contains(k)) {
                        log.debug("Extracting $k")
                        doclist += extractEntities(k, entities, doc.identifier)
                    }
                }
            }

            json?.about?.inScheme?.each { k, entities ->
                if (entitiesToExtract.contains(k)) {
                        log.debug("Extracting $k")
                        doclist += extractEntities(k, entities, doc.identifier)
                }
            }
            log.debug("Extraction results: $doclist")
        
        return doclist
    }

    List<IndexDocument> extractEntities(def key, def entities, def id) {
        List<IndexDocument> entityDocList = []
        if (entities instanceof List) {
            for (entity in entities) {
                def entityDoc = createEntityDoc(entity, id, 1, true)
                if (entityDoc) {
                    entityDocList << entityDoc
                }
            }
        } else if (entities instanceof Map) {
            def entityDoc = createEntityDoc(entities, id, 1, true)
            if (entityDoc) {
                entityDocList << entityDoc
            }
        }
        return entityDocList
    }

    IndexDocument createEntityDoc(def entityJson, def docId, def prio, def slugifyId) {
        try {
            def type = entityJson["@type"]
            String pident = docId
            if (slugifyId) {
                def authPoint = entityJson[(authPoint[(type)])]
                pident = slugify(authPoint, docId, type)
                entityJson["extractedFrom"] = ["@id":docId]
            } else {
                entityJson["@id"] = pident
            }
            entityJson["recordPriority"] = prio
            entityJson.get("unknown", null) ?: entityJson.remove("unknown")
            return new IndexDocument().withData(mapper.writeValueAsBytes(entityJson)).withContentType("application/ld+json").withIdentifier(pident).withType(type)
        } catch (Exception e) {
            log.debug("Could not create entitydoc ${e}")
            return null
        }
    }

    String slugify(String authAccPoint, URI identifier, String entityType) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/" + entityType + "/" + URLEncoder.encode(authAccPoint))
    }
}
