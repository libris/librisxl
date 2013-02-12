package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class  JsonLD2MarcConverter extends MarcCrackerAndLabelerIndexFormatConverter implements FormatConverter {

    String requiredContentType = "application/json"
    def marcref

    JsonLD2MarcConverter() {
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
    }

    def mapDocument(def injson) {
        def fields = [:]
        def idstr = injson?.get("@id").split("/")
        if (idstr) {
         fields["001"] = idstr[idstr.length - 1]
        }
        fields["005"] = injson?.get("dateAndTimeOfLatestTransaction").replaceAll("^\\d.", "")
    }

    def mapIsbn(injson) {
        return injson
    }

    def mapPerson(injson) {
        return injson
    }

}
