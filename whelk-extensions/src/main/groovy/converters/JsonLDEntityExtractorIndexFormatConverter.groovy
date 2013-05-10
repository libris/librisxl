package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()

    List<Document> doConvert(Document doc) {
        def doclist = [doc]
        def json = mapper.readValue(doc.dataAsString, Map)
        def authList = json.about?.instanceOf?.authorList
        if (authList instanceof List) {
            for (person in authList) {
                String pident = slugify(person["authorizedAccessPoint"], doc.identifier)
                person["@id"] = pident
                person["recordPriority"] = 0
                doclist << new BasicDocument().withData(mapper.writeValueAsBytes(person)).withContentType("application/ld+json").withIdentifier(pident).tag("entityType", person["@type"]).withLink(doc.identifier, "authorOf")
            }
        } else if (authList instanceof Map) {
            String pident = slugify(person["authorizedAccessPoint"], doc.identifier)
            authList["@id"] = pident
            authList["recordPriority"] = 0
            doclist << new BasicDocument().withData(mapper.writeValueAsBytes(authList)).withContentType("application/ld+json").withIdentifier(pident).tag("entityType", authList["@type"]).withLink(doc.identifier, "authorOf")
        }
        if (json["@type"]) {
            log.debug("Record has a @type. Adding to entity recordtype.")
            json["recordPriority"] = 1
            json.remove("unknown")
            doclist << new BasicDocument().withData(mapper.writeValueAsBytes(json)).withContentType("application/ld+json").withIdentifier(doc.identifier).tag("entityType", json["@type"])
        }
        log.trace("Extraction results: $doclist")
        return doclist
    }

    String slugify(String authAccPoint, URI identifier) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/person/"+URLEncoder.encode(authAccPoint))
    }
}
