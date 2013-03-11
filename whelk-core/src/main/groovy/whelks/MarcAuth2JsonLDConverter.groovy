package se.kb.libris.whelks.plugin

import se.kb.libris.conch.Tools
import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class MarcAuth2JsonLDConverter extends BasicFormatConverter implements FormatConverter, IndexFormatConverter {

    String requiredContentType = "application/json"
    String requiredFormat = "marc21"
    final String RTYPE = "auth"
    ObjectMapper mapper

    def marcref
    def marcmap

    MarcAuth2JsonLDConverter() {
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
        is = this.getClass().getClassLoader().getResourceAsStream("marcmap.json")
        this.marcmap = mapper.readValue(is, Map)
    }

    def mapField(outjson, code, fjson, docjson) {
        if (marcref[RTYPE].mapping[code]) {
            def usecode = (marcref[RTYPE].mapping[code] instanceof Map ? code : marcref[RTYPE].mapping[code])
            marcref[RTYPE].mapping[usecode].each { property, fieldcode ->
                getMarcField(fieldcode, docjson).each {
                    if (it) {
                        log.trace("inserting at $property: $it")
                        outjson = Tools.insertAt(outjson, property, it)
                    }
                }
            }

        } else if (marcref[RTYPE].process[code]) {
            log.trace("call method ${marcref[RTYPE].process[code]}")
            def mapping = "${marcref[RTYPE].process[code].method}"(outjson, code, fjson, docjson)
            outjson = Tools.insertAt(outjson, marcref[RTYPE].process[code].level, mapping)
        } else {
            outjson = dropToRaw(outjson, [(code):fjson])
        }
        log.trace("outjson: $outjson")
        return outjson
    }

    List<Document> doConvert(Document doc) {
        def injson = doc.dataAsJson
        def outjson = [:]

        for (field in injson.fields) {
            it.each { code, fjson ->
                mapField(outjson, code, fjson, injson)
            }
        }

        return [new BasicDocument(doc).withData(mapper.writeValueAsBytes(outjson)).withFormat("jsonld")]
    }


    // Utility methods
    def dropToRaw(outjson, marcjson) {
        outjson = Tools.insertAt(outjson, "unknown.fields", marcjson)
    }

    def getMarcField(fieldcode, marcjson, joinChar=" ") {
        def (field, code) = fieldcode.split(/\./, 2)
        def values = []
        log.trace("field: $field, code: $code, codes: ${code.toList()}")
        marcjson.fields.each {
            it.each { f, v ->
                if (f == field) {
                    def codeValues = []
                    v.subfields.each { s ->
                        s.each {sc, sv ->
                            if (sc in code.toList()) {
                                codeValues << sv
                            }
                        }
                    }
                    values << codeValues.join(joinChar)
                }
            }
        }
        log.trace("extracted values: $values")
        return values
    }

    // Mapping methods
    def mapPerson(outjson, code, fjson, marcjson) {
        return ["authoritativeName":"Foo Bar","birthDate":"epoch"]
    }
}

