package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/json"
    String requiredFormat = "jsonld"
    ObjectMapper mapper = new ObjectMapper()

    List<Document> doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)
        def date = json.about?.get("dateOfPublication")
        def cleaned_date
        if (date && date instanceof String) {
            cleaned_date = date.replaceAll("[^\\d-0]", "")
        } else if (date && date instanceof List) {
            cleaned_date = date[0].replaceAll("[^\\d-0]", "")
        }
        if (cleaned_date) {
           json["about"]["dateOfPublication"] = cleaned_date
           doc = doc.withData(mapper.writeValueAsBytes(json))
        }
        return [doc]
    }
}
