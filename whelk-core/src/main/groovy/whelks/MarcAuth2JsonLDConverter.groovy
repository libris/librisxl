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
                log.trace("Examining mapping: $property -- $fieldcode")
                if (fieldcode instanceof String) {
                    if (fieldcode.startsWith(usecode)) {
                        def v = getMarcValueFromField(fieldcode, fjson)
                        if (v) {
                            Tools.insertAt(outjson, property, v)
                        }
                    } else {
                        getMarcField(fieldcode, docjson).each {
                            if (it) {
                                log.trace("inserting at $property: $it")
                                outjson = Tools.insertAt(outjson, property, it)
                            }
                        }
                    }
                } else if (fieldcode instanceof Map) {
                    def obj = [:]
                    fieldcode.each { item, fcode ->
                        def v = getMarcValueFromField(fcode, fjson)
                        if (v) {
                            obj[(item)] = v
                        }
                    }
                    outjson = Tools.insertAt(outjson, property, obj)
                }
            }

        } else if (marcref[RTYPE].process[code]) {
            log.trace("call method ${marcref[RTYPE].process[code]}")
            def mapping = "${marcref[RTYPE].process[code].method}"(outjson, code, fjson, docjson)
            outjson = Tools.insertAt(outjson, marcref[RTYPE].process[code].level, mapping)
        } else if (!(marcref[RTYPE].handled_elsewhere[code])) {
            outjson = dropToRaw(outjson, [(code):fjson])
        }
        //log.trace("outjson: $outjson")
        return outjson
    }

    List<Document> doConvert(Document doc) {
        def injson = doc.dataAsJson
        def outjson = [:]


        for (field in injson.fields) {
            field.each { code, fjson ->
                outjson = mapField(outjson, code, fjson, injson)
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
                    log.trace("calling getvalue at $f")
                    values << getMarcValueFromField(code, v, joinChar)
                }
            }
        }
        log.trace("extracted values: $values")
        return values
    }

    def getMarcValueFromField(code, fieldjson, joinChar=" ") {
        def codeValues = []
        log.trace("getMarcValueFromField: $code - $fieldjson")
        fieldjson.subfields.each { s ->
            s.each {sc, sv ->
                if (sc in code.toList()) {
                    codeValues << sv
                }
            }
        }
        log.trace("codeValues: $codeValues")
        return codeValues.join(joinChar)
    }

    // Mapping methods
    def mapPerson(outjson, code, fjson, marcjson) {
        log.trace("person json: $fjson")
        def person = [:]
        if ((code as int) % 100 == 0) {
            person["@type"] = "Person"
        }
        if ((code as int) % 110 == 0) {
            person["@type"] = "Organization"
        }
        if ((code as int) % 111 == 0) {
            person["@type"] = "Conference"
        }
        def name = getMarcValueFromField("a", fjson)
        def numeration = getMarcValueFromField("b", fjson)
        def title = getMarcValueFromField("c", fjson)
        def dates = getMarcValueFromField("d", fjson)
        person["authoritativeName"] = name.replaceAll(/,$/, "").trim()
        person["authorizedAccessPoint"] = name.replaceAll(/,$/, "").trim()
        if (numeration) {
            person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + " " + numeration
            person["numeration"] = numeration
        }
        if (title) {
            person["titlesAndOtherWordsAssociatedWithName"] = title
            person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + ", " + title

        }
        if (dates) {
            person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + ", " + dates
            dates = dates.split(/-/)
            person["birthYear"] = dates[0]
            if (dates.size() > 1) {
                person["deathYear"] = dates[1]
            }
        }
        if (fjson.ind1 == "1" && person["authoritativeName"] =~ /, /) {
            person["givenName"] = person["authoritativeName"].split(", ")[1]
            person["familyName"] = person["authoritativeName"].split(", ")[0]
            person["name"] =  person["givenName"] + " " + person["familyName"]
        } else {
            person["name"] = person["authoritativeName"]
        }
        def altLabels = []
        for (f in ["400.a","410.a","411.a","500.a","510.a","511.a"]) {
            altLabels.addAll(getMarcField(f, marcjson))
        }
        if (altLabels) {
            person["label"] = altLabels
        }
        log.trace("person: $person")
        return person
    }
}
