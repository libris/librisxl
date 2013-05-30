package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def entityLists = ["person": json.about?.instanceOf?.authList, "subject": json.about?.instanceOf?.subjectList]
    def entityLinks = ["person": "authorOf", "subject": "subject"]

    List<Document> doConvert(Document doc) {
        def doclist = [doc]
        def json = mapper.readValue(doc.dataAsString, Map)

        entityLists.each { k, entities ->
                    if (entities) {
                        if (entities instanceof List) {
                            for (entity in entities) {
                                String pident = slugify(entity["authorizedAccessPoint"], doc.identifier, k)
                                entity["@id"] = pident
                                entity["recordPriority"] = 0
                                doclist << new BasicDocument().withData(mapper.writeValueAsBytes(entity)).withContentType("application/ld+json").withIdentifier(pident).tag("entityType", entity["@type"]).withLink(doc.identifier, entityLinks[(k)])
                            }
                        } else if (entities instanceof Map) {
                            if (entities.get("authorizedAccessPoint", null)) {
                                String pident = slugify(entities["authorizedAccessPoint"], doc.identifier, k)
                            }
                            entities["@id"] = pident
                            entities["recordPriority"] = 0
                            doclist << new BasicDocument().withData(mapper.writeValueAsBytes(entities)).withContentType("application/ld+json").withIdentifier(pident).tag("entityType", entities["@type"]).withLink(doc.identifier, entityLinks[(k)])
                        }
                   }
          
            }

        //def authList = json.about?.instanceOf?.authorList
        
        if (json["@type"]) {
            log.debug("Record has a @type. Adding to entity recordtype.")
            json["recordPriority"] = 1
            json.remove("unknown")
            doclist << new BasicDocument().withData(mapper.writeValueAsBytes(json)).withContentType("application/ld+json").withIdentifier(doc.identifier).tag("entityType", json["@type"])
        }
        log.trace("Extraction results: $doclist")
        return doclist
    }

    String slugify(String authAccPoint, URI identifier, String entityType) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/" + entityType + "/" + URLEncoder.encode(authAccPoint))
    }
}
