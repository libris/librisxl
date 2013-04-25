package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()

    List<Document> doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)
        def date = json.about?.get("dateOfPublication")
        if (date) {
            def cleaned_date
            if (date instanceof String) {
                cleaned_date = date
            } else if (date instanceof List) {
                cleaned_date = date[0]
            }
            json["about"]["dateOfPublication"] = cleaned_date.replaceAll("[^\\d-0]", "")
        }
        def title = json.about?.instanceOf?.get("title")
        if (title) {
           def cleaned_title
           if (title instanceof String) {
              if (title[-1].equals("/")) {
                  cleaned_title = title[0..-2]
              }
              //json["about"]["instanceOf"]["title"] = cleaned_title
           }
        }
        doc = doc.withData(mapper.writeValueAsBytes(json))
        //TODO: clean up interpunction 260, 300
        return [doc]
    }
}
