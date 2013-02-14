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
        def fields = []
        def leader = ["leader": "theleader"]
        def isbnParts = [:]
        def idstr = injson?.get("@id").split("/")
        if (idstr) {
            fields << ["001": idstr[idstr.length - 1]]
        }
        fields << ["005": injson?.get("dateAndTimeOfLatestTransaction").replaceAll("^\\d.", "")]
        def collection = injson["describes"]["expression"]
        collection.each { key, value ->
            log.trace("key: $key value: $value")
            switch(key) {
                case "authorList":
                    log.trace("authorList value: $value")
                    value.each {
                        fields << ["100": mapPerson(it)]
                    }
                    break
                case "isbn":
                case "isbnRemainder":
                    //fields << ["020": mapIsbn([injson["isbn"]] << injson["isbnRemainder"])]
                    isbnParts << it
                    break
                default:
                    value.each { ky, ve ->
                        fields << [(ky): (ve)]
                    }
                    break
            }
        }
        if (isbnParts.length > 1) {
            fields << ["020": mapIsbn(isbnParts)]                  
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
        def isbnRemainder = ""
        if (injson["isbnRemainder"]) {
            isbnRemainder = " " + injson["isbnRemainder"]
        }
        if (injson["isbn"]) {
            marcField["subfields"] << ["a": injson["isbn"] + isbnRemainder]
        }
        if (injson["termsOfAvailability"]) {
            marcField["subfields"] << ["c": injson["termsOfAvailability"]["literal"]]
        }
        if (injson[Marc2JsonLDConverter.RAW_LABEL]) {
            injson[Marc2JsonLDConverter.RAW_LABEL]["020"]["subfields"].each { it.each { k, v ->
                    marcField["subfields"] << [(k):(v)]
                }
            }
        }
        return marcField
    }

    def mapPerson(injson) {
        def marcField = createMarcField("0", " ")
        def name = injson?.get("name")
        def date
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
                case "dateOfDeath":
                    value.each { k, v ->
                        if (k == "@type") {
                            def dateType = v
                        } else if (k == "@value") {
                            if (key == "dateOfBirth") {
                                date = v + "-"
                            } else if (key == "dateOfDeath") {
                                date = date + v
                            }
                        }
                    }
                    break
            }
        }
        if (name) {
            def subA = [:]
            subA["a"] = name
            marcField["subfields"] << subA
        }
        if (date) {
            def subD = [:]
            subD["d"] = date
            marcField["subfields"] << subD
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
        //TODO: switch tag -> indicators
        //return tag?
        //more than one marcfield?
        def marcField = createMarcField(" ", " ")
        injson.each { key, value ->
            if (key == Marc2JsonLDConverter.RAW_LABEL) {
                value.each { k, v ->
                    marcField = v
                }
            } else {
                marcref.fields.each {
                    it.value.each { k, v ->
                        v.each {
                            if (key.trim().equals(it)) {
                                //tag = it.key
                                marcField["subfields"] << [(k):(value)]
                            }
                        }
                    }
                }
            }
        }
        return marcField
    }
    
}

