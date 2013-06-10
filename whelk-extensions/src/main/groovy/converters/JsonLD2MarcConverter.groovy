package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.basic.BasicFormatConverter
import se.kb.libris.whelks.resource.Document

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class  JsonLD2MarcConverter extends BasicFormatConverter {

    String requiredContentType = "application/ld+json"
    static final String RAW_LABEL = "unmapped_jsonld"
    def marcref
    def mapper

    JsonLD2MarcConverter() {
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
    }

    static main(args) {
        def objMapper = new ObjectMapper()
        def source = new File(args[0])
        def destDir = new File(args[1])
        def dest = new File(destDir, source.name)  
        log.info("source: $source")
        log.info("destDir: $destDir")
        def inData = source.withInputStream { objMapper.readValue(it, Map) }
        log.info("${inData}")
        def converter = new JsonLD2MarcConverter()
        def outjson = converter.mapDocument(inData)
        dest << objMapper.defaultPrettyPrintingWriter().writeValueAsString(outjson).getBytes("utf-8")
    }
    
    Document doConvert(Document doc) {
        if (doc instanceof Document) {
            return mapDocument(doc.getDataAsJson())
        }
        return null
    }
    
    
    def mapDocument(injson) {
        def out = [:]
        def fields = []
        def leader = ["leader": "theleader"]
        def isbnParts = []
        //def collectedParts = []
        def titleParts = [:]
        def publicationParts = [:]
        def physicalDescParts = [:]
        def idstr = injson?.get("@id").split("/")
        if (idstr) {
            fields << ["001": idstr[idstr.length - 1]]
        }
        fields << ["005": injson?.get("modified")?.replaceAll("^\\d.", "")]

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
        }
        /*marc_refs["bib"]["mapping"].each { tag, v ->
            v.each
        }*/

        injson["about"].each { k, v ->
            switch(k) {
                case "placeOfPublication":
                case "publisher":
                case "pubDate":
                case "placeOfManufacture":
                    publicationParts[k] = v
                    break
                case "extent":
                case "baseMaterial":
                case "dimensions":
                    physicalDescParts[k] = v
                    //collectedParts << [k: v]
                    break
                case "isbn":
                case "isbnNote":
                case "identifier":
                case "termsOfAvailability":
                    isbnParts << [(k): v]
                    break
                default://kolla om ej djupare nivå
                    fields << mapDefaultGetWithTag([(k): (v)])
                    break
            }
        }

        def collection = injson["about"]["instanceOf"]
        collection.each { key, value ->
            log.trace("key: $key value: $value")
            switch(key) {
                case "authorList":
                    value.each {
                        fields << ["100": (mapPerson(it))]
                    }
                    break
                case "title":
                case "titleRemainder":
                case "statementOfResponsibility":
                    titleParts[key] = value
                    //collectedParts << [key: value]
                    break
                default:
                    fields << mapDefaultGetWithTag([(key): (value)])
                    break
            }
        }
        if (isbnParts.size() > 1) {
            fields << ["020": (mapIsbn(isbnParts))]
        }
        if (titleParts.size() > 1) {
            fields << ["245": mapDefault(titleParts)]
        }
        if (publicationParts.size() > 1) {
            fields << ["260": mapDefault(publicationParts)]
        }
        if (physicalDescParts.size() > 1) {
            fields << ["300": mapDefault(physicalDescParts)]
        }
        out["fields"] = fields
        log.info("Marc out:\n" + out)
        return out
    }
    
    //TODO: mapDocument to use marc_refs
    //TODO: mapDefault to use marc_refs for tags

    def createMarcField(ind1, ind2) {
        def marcField = [:]
        marcField["ind1"] = ind1
        marcField["ind2"] = ind2
        marcField["subfields"] = []
        return marcField
    }

    def mapIsbn(injson) {
        def marcField = createMarcField(" ", " ")
        //TODO:020 isbn identifierscheme
        if (injson["identifier"]) {
            marcField["subfields"] << ["a": injson["identifier"]["identifierValue"]]
        } else if (injson["isbn"]) {
            marcField["subfields"] << ["a": injson["isbn"]]
        }
        if (injson["termsOfAvailability"]) {
            marcField["subfields"] = ["c": injson["termsOfAvailability"]]
        }
        /*if (injson[Marc2JsonLDConverter.RAW_LABEL] != null) {
            log.trace("inrawlabel")
            marcField = createMarcFieldFromRawInput(injson[Marc2JsonLDConverter.RAW_LABEL])
        }*/
        return marcField
    }

    def mapPerson(injson) {
        log.trace("mapperson injson: $injson")
        def marcField = createMarcField("0", " ")
        def name = injson?.get("name")
        def date = [:]
        injson.each { key, value ->
            switch (key) {
                case "familyName":
                    marcField["ind1"] = "1"
                    name = value
                    break
                case "givenName":
                    name = name + ", " + value
                    break
                case "birthYear":
                    date[key] = value + "-"
                case "deathYear":
                    date[key] = value
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
            subD["d"] = "${date["birthYear"]} + ${date["deathYear"]}"
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
        log.trace("mapdefault injson: $injson")
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
            return [(RAW_LABEL): (injson)]
        }
        def out = [(outTag): (marcField)]
        log.trace("mapdefault outjson: $out")
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

