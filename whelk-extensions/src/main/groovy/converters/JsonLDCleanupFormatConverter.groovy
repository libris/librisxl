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
        def title = json.about.instanceOf?.get("title")
        def titleRemainder = json.about.instanceOf?.get("titleRemainder")
        def statementOfResponsibility = json.about.instanceOf?.get("statementOfResponsibility")
        def cleaned_title = title
        def cleaned_titleRemainder = titleRemainder
        if (title) {
           if (titleRemainder && title[-1].equals(":")) {
               cleaned_title = title[0..-2].trim()
           }
           if (statementOfResponsibility) {
               if (title[-1].equals("/")) {
                    cleaned_title = title[0..-2].trim()
               }
               if (titleRemainder && titleRemainder[-1].equals("/")) {
                    cleaned_titleRemainder = titleRemainder[0..-2].trim()
               }
           }
           json["about"]["instanceOf"]["title"] = cleaned_title
           json["about"]["instanceOf"]["titleRemainder"] = cleaned_titleRemainder
        }
        
        doc = doc.withData(mapper.writeValueAsBytes(json))
        //TODO: clean up interpunction 260, 300
        return doc
    }
}