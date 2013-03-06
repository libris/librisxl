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
        for (person in json.about.instanceOf.authorList) {
            i++
            String pident = "${doc.identifier.toString()}/person/$i"
            person["@id"] = pident
            person["authorOf"] = doc.identifier
            log.debug("Found person: $person")
            doclist << new BasicDocument().withData(mapper.writeValueAsBytes(person)).withFormat("jsonld").withContentType("application/json").withIdentifier(pident).tag("entityType", person["@type"])
        }
        return doclist
    }
}
