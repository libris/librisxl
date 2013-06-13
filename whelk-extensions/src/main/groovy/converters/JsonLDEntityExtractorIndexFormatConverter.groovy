package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def authPoint = ["Person" : "controlledLabel", "Concept" : "prefLabel"]

    List<IndexDocument> doConvert(Resource doc) {
        List<IndexDocument> doclist = [new IndexDocument(doc)]
        def json = mapper.readValue(doc.dataAsString, Map)
        def indexTypeLists = [
            "creator": json.about?.instanceOf?.creator,
            "contributor" : json.about?.instanceOf?.contributor,
            "subject" : json.about?.instanceOf?.subject
        ]

        indexTypeLists.each { k, entities ->
            if (entities) {
                if (entities instanceof List) {
                    for (entity in entities) {
                        def entityDoc = createEntityDoc(entity, doc.identifier, k)
                        if (entityDoc) {
                            doclist << entityDoc
                        }    
                    }
                } else if (entities instanceof Map) {
                    def entityDoc = createEntityDoc(entities, doc.identifier, k)
                    if (entityDoc) {
                        doclist << entityDoc
                    }
               }
            }
        }
        if (json["@type"]) {
            log.debug("Record has a @type. Adding to entity recordtype.")
            json["recordPriority"] = 1
            json.remove("unknown")
            doclist << new IndexDocument(doc).withData(mapper.writeValueAsBytes(json)).withContentType("application/ld+json").withType(json["@type"])
        }
        log.debug("Extraction results: $doclist")
        return doclist
    }

    IndexDocument createEntityDoc(def entityJson, def docId, def linkType) {
        try {
            def type = entityJson["@type"]
            def authPoint = entityJson[(authPoint[(type)])]
            String pident = slugify(authPoint, docId, type)
            entityJson["@id"] = pident
            entityJson["recordPriority"] = 0
            return new IndexDocument().withData(mapper.writeValueAsBytes(entityJson)).withContentType("application/ld+json").withIdentifier(pident).withType(type)
        } catch (Exception e) {
            return null
        }
    }

    String slugify(String authAccPoint, URI identifier, String entityType) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/" + entityType + "/" + URLEncoder.encode(authAccPoint))
    }
}
