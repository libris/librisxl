package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/json"
    String requiredFormat = "jsonld"
    ObjectMapper mapper = new ObjectMapper()

    List<Document> doConvert(Document doc) {
        def doclist = [doc]
        def json = mapper.readValue(doc.dataAsString, Map)
        int i = 0
        def authList = json.about?.instanceOf?.authorList
        if (authList instanceof List) {
            for (person in authList) {
                i++
                String pident = "${doc.identifier.toString()}/person/$i"
                person["@id"] = pident
                person["authorOf"] = doc.identifier
                doclist << new BasicDocument().withData(mapper.writeValueAsBytes(person)).withFormat("jsonld").withContentType("application/json").withIdentifier(pident).tag("entityType", person["@type"])
            }
        } else if (authList instanceof Map) {
            String pident = "${doc.identifier.toString()}/person/$i"
            authList["@id"] = pident
            authList["authorOf"] = doc.identifier
            doclist << new BasicDocument().withData(mapper.writeValueAsBytes(authList)).withFormat("jsonld").withContentType("application/json").withIdentifier(pident).tag("entityType", authList["@type"])
        }
        if (json["@type"]) {
            log.debug("Record has a @type. Adding to entity recordtype.")
            doclist << new BasicDocument(doc).tag("entityType", json["@type"])
        }
        return doclist
    }
}
