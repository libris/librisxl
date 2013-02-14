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
        injson["describes"]["expression"].each { key, value ->
            switch(key) {
                case "authorList":
                    value.each {
                        fields["100"] = mapPerson(it)
                    }
                    break
                case "isbnRemainder":
                case "isbn":
                    fields["020"] = mapIsbn([injson["isbn"]] << injson["isbnRemainder"])
                    break
                case (Marc2JsonLDConverter.RAW_LABEL):
                    value.each { k, v ->
                        fields[k] = v
                    }
                    break
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

    def mapIsbn(injson) {
        def marcField = createMarcField(" ", " ")
        def subfields = [:]
        def isbnRemainder = ""
        if (injson["isbnRemainder"]) {
            isbnRemainder = " " + injson["isbnRemainder"]
        }
        if (injson["isbn"]) {
            subfields["a"] = injson["isbn"] + isbnRemainder
        }
        if (injson["termsOfAvailability"]) {
            subfields["c"] = injson["termsOfAvailability"]["literal"]
        }
        if (injson[Marc2JsonLDConverter.RAW_LABEL]) {
            injson[Marc2JsonLDConverter.RAW_LABEL]["020"]["subfields"][0].each { k, v ->
                subfields[k] = v
            }
        }
        marcField["subfields"] << subfields
        return marcField
    }

    def mapPerson(injson) {
        def marcField = createMarcField("0", " ")
        def name = injson?.get("name")
        injson.each { key, value ->
            switch (key) {
                case "surname":
                    marcField["ind1"] = "1"
                    name = value
                    break
                case "givenName":
                    name = name + ", " + value
                    break
                case "dateOfBirth":
                    def subD = [:]
                    subD["d"] = value
                    marcField["subfields"] << subD
                    break
                case "dateOfDeath":
                    def subX = [:]
                    subX["d"] = value
                    marcField["subfields"] << subX
                    break
            }
        }
        if (name) {
            def subA = [:]
            subA["a"] = name
            marcField["subfields"] << subA
        }
        if (injson?.get(Marc2JsonLDConverter.RAW_LABEL)) {
            injson[Marc2JsonLDConverter.RAW_LABEL].each { key, value ->
                marcField["ind1"] = injson[Marc2JsonLDConverter.RAW_LABEL][key]["ind1"]
                marcField["ind2"] = injson[Marc2JsonLDConverter.RAW_LABEL][key]["ind2"]
                injson[Marc2JsonLDConverter.RAW_LABEL][key]["subfields"].each { it.each { k, v ->
                        marcField["subfields"] << [(k):(v)]
                
                    }
                }
            }
        }
        return marcField
    }

    def mapDefault(injson) {
        def marcFields = []
        def indicators = []
        def tag
        def subfield
        //TODO: switch tag -> indicators
        def subfields = [:]
        injson.each { key, value ->
            marcref.each { it.each { k, v ->
              v.eachWithIndex { item, idx ->
                if (key == item) {
                    def marcField = createMarcField(" ", " ")
                    tag = it.getKey()
                    marcField["subfields"] << [(k):(value)]
                    marcFields << marcField
                }
              }

            }
          }
        }
        return marcFields
    }
    
}

