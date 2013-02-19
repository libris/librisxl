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
        def out = [:]
        def fields = []
        def leader = ["leader": "theleader"]
        def isbnParts = []
        def idstr = injson?.get("@id").split("/")
        if (idstr) {
            fields << ["001": idstr[idstr.length - 1]]
        }
        fields << ["005": injson?.get("dateAndTimeOfLatestTransaction").replaceAll("^\\d.", "")]

        if (injson[Marc2JsonLDConverter.RAW_LABEL]) {
            injson[Marc2JsonLDConverter.RAW_LABEL]["fields"].each { it.each { key, value ->
                  def tag = key
                  def marcField = createMarcField(" ", " ")
                  value.each { k, v ->
                      switch(k) {
                          case "subfields":
                                v.each { it.each { i, j ->
                                    marcField["subfields"] << [(i):(j)]
                                  }
                                }
                                break
                          default:
                                marcField[k] = v
                      }
                  }
                 fields << [(tag): (marcField)]
                }
             }
             //fields << ["raw": (createMarcFieldFromRawInput(injson[Marc2JsonLDConverter.RAW_LABEL]))]
        }

        injson["describes"].each { k, v ->
            switch(k) {
                default:
                    fields << mapDefaultGetWithTag([(k): (v)])
                    break
            }
        }

        def collection = injson["describes"]["expression"]
        collection.each { key, value ->
            log.trace("key: $key value: $value")
            switch(key) {
                case "authorList":
                    value.each {
                        fields << ["100": (mapPerson(it))]
                    }
                    break
                case "isbn":
                case "isbnData":
                case "termsOfAvailability":
                    //fields << ["020": mapIsbn([injson["isbn"]] << injson["isbnRemainder"])]
                    isbnParts << [(key): (value)]
                    break
                case "titleProper":
                    fields << ["245": mapDefault([(key): (value)])]
                    break
                default:
                    fields << mapDefaultGetWithTag([(key): (value)])
                    break
            }
        }
        if (isbnParts.size() > 1) {
            fields << ["020": (mapIsbn(isbnParts))]
        }

        out["fields"] = fields
        log.trace("Marc out:\n" + out)
        return out
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
        def isbnData = ""
        if (injson["isbnData"]) {
            isbnData = " " + injson["isbnData"]
        }
        if (injson["isbn"]) {
            marcField["subfields"] << ["a": injson["isbn"] + isbnData]
        }
        if (injson["termsOfAvailability"]) {
            marcField["subfields"] << ["c": injson["termsOfAvailability"]["literal"]]
        }
        if (injson[Marc2JsonLDConverter.RAW_LABEL]) {
            marcField = createMarcFieldFromRawInput(injson[Marc2JsonLDConverter.RAW_LABEL])
        }
        return marcField
    }

    def mapPerson(injson) {
        log.trace("mapperson injson: $injson")
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
            marcField = createMarcFieldFromRawInput(injson[Marc2JsonLDConverter.RAW_LABEL])
        }
        return marcField
    }

    def mapDefault(injson) {
        def out
        mapDefaultGetWithTag(injson).each { key, value ->
            out = value
        }
        return out
    }
    
    def mapDefaultGetWithTag(injson) {
        //TODO: switch tag -> indicators
        //more than one marcfield?
        def marcField = createMarcField(" ", " ")
        def outTag = "no tag found"
        def isControlField = false
        injson.each { key, value ->
            if (key == Marc2JsonLDConverter.RAW_LABEL) {
                value["fields"].each { it.each { k, v ->
                        outTag = k
                        marcField = v
                    }
                }
            } 
            else {
                marcref.fields.each {
                    def tag = it.key
                    if (tag.matches(/00\d/)) {
                        isControlField = true
                    }
                    if (it.value instanceof Map) {
                        it.value.each { k, v ->
                            v.each {
                                if (key.trim().equals(it)) {
                                    outTag = tag
                                    if (value instanceof List) {
                                        value.each {
                                            marcField["subfields"] << [(k):(it)]
                                        }
                                    } else if (value instanceof Map) {
                                        value.each { x, y ->
                                            switch (x) {
                                                case "label":
                                                    marcField["subfields"] << [(k):(y)]
                                                    break
                                                case "@type":
                                                    marcField["subfields"] << [(k):(value["@value"])]
                                                    break
                                            }
                                        }
                                    } else {
                                        marcField["subfields"] << [(k):(value)]
                                    }
                                }
                            }
                        }
                    } else if (key == it.value) {
                        if (isControlField) {
                            marcField = value
                        } else {
                            marcField[it.key] = value
                        }
                    }
                }
            }
        }
        if (outTag == "no tag found") {
            return [("injson i could not map"): (injson)]
        }
        def out = [(outTag): (marcField)]
        return out
    }

    def createMarcFieldFromRawInput(injson) {
        def marcField = createMarcField(" ", " ")
        injson["fields"].each { it.each { key, value ->
              value.each { k, v ->
                   switch(k) {
                       case "subfields":
                            v.each { it.each { x, y ->
                                 marcField["subfields"] << [(x):(y)]
                                }
                            }
                            break
                       default:
                            marcField[k] = v
                            break
                    }
               }
            }
         }
         return marcField
    }
}

