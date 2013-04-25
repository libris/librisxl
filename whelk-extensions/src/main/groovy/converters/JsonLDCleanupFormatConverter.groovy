package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupFormatConverter extends BasicFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()

    Document doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)
        def title = json.about?.instanceOf?.get("title")
        if (title) {
           def cleaned_title
           if (title instanceof String) {
              if (title[-1].equals("/")) {
                  cleaned_title = title[0..-2].trim()
              }
              json["about"]["instanceOf"]["title"] = cleaned_title
           }
        }
        doc = doc.withData(mapper.writeValueAsBytes(json))
        //TODO: clean up interpunction 260, 300
        return doc
    }
}