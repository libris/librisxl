package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()

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
                        if (k == "creator" && entity.get("controlledLabel", null)) {
                            doclist << createEntityDoc("person", entity, entity["controlledLabel"], doc.identifier, "creatorOf")
                        } else if (k == "contributor" && entity.get("controlledLabel", null)) {
                            doclist << createEntityDoc("person", entity, entity["controlledLabel"], doc.identifier, "contributor")
                        } else if (k == "concept" && entity.get("prefLabel", null)) {
                            doclist << createEntityDoc("concept", entity, entity["prefLabel"], doc.identifier, "subject")
                        }
                    }
                } else if (entities instanceof Map) {
                    /*if (entities.get("controlledLabel", null)) {
                    doclist << createEntityDoc(k, entities, doc.identifier, entityLinks[(k)])
                    }*/
                    if (k == "creator" && entity.get("controlledLabel", null)) {
                        doclist << createEntityDoc("person", entity, entity["controlledLabel"], doc.identifier, "creatorOf")
                    } else if (k == "contributor" && entity.get("controlledLabel", null)) {
                        doclist << createEntityDoc("person", entity, entity["controlledLabel"], doc.identifier, "contributor")
                    } else if (k == "concept" && entity.get("prefLabel", null)) {
                        doclist << createEntityDoc("concept", entity, entity["prefLabel"], doc.identifier, "subject")
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

    IndexDocument createEntityDoc(def type, def entityJson, def authPoint, def docId, def linkType) {
        String pident = slugify(authPoint, docId, type)
        entityJson["@id"] = pident
        entityJson["recordPriority"] = 0
        return new IndexDocument().withData(mapper.writeValueAsBytes(entityJson)).withContentType("application/ld+json").withIdentifier(pident).withType(type)
    }

    String slugify(String authAccPoint, URI identifier, String entityType) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/" + entityType + "/" + URLEncoder.encode(authAccPoint))
    }
}
