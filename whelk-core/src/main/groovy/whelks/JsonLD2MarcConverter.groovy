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

    def mapDocument(injson) {
        def fields = [:]
        def idstr = injson?.get("@id").split("/")
        if (idstr) {
         fields["001"] = idstr[idstr.length - 1]
        }
        fields["005"] = injson?.get("dateAndTimeOfLatestTransaction").replaceAll("^\\d.", "")
        injson.each { key, value ->
            switch(key) {
                case "isbnRemainder":
                case "isbn":
                    fields << mapIsbn([injson["isbn"]] << injson["isbnRemainder"])
                    break
                case "authorList":
                    break
                default:
                    return fields
            }

        }
        return fields
    }

    def createMarcField(ind1, ind2) {
        def marcField = [:]
        marcField["ind1"] = ind1
        marcField["ind2"] = ind2
        marcField["subfields"]= []
        return marcField
    }

    def addSubfield(marcField, code, value) {
        return marcField["subfields"][code] = value
    }

    def mapIsbn(injson) {
        def marcField = createMarcField(" ", " ")
        def subfield = [:]
        def isbnRemainder = ""
        if (injson["isbnRemainder"]) {
            isbnRemainder = " " + injson["isbnRemainder"]
        }
        if (injson["isbn"]) {
            subfield["a"] = injson["isbn"] + isbnRemainder
        }
        if (injson["termsOfAvailability"]) {
            subfield["c"] = injson["termsOfAvailability"]["literal"]
        }
        marcField["subfields"] << subfield
        return marcField
    }

    def mapPerson(injson) {
        def out = [:]
        return out
    }

}

