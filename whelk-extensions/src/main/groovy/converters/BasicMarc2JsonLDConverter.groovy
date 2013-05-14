package se.kb.libris.whelks.plugin

import java.text.SimpleDateFormat

import se.kb.libris.conch.Tools
import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class BasicMarc2JsonLDConverter extends BasicFormatConverter implements FormatConverter {

    String requiredContentType = "application/x-marc-json"
    String RTYPE
    ObjectMapper mapper

    def marcref
    def marcmap
    def thesauri

    def convertedCodes = [:]

    BasicMarc2JsonLDConverter(String rt) {
        this.RTYPE = rt
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
        is = this.getClass().getClassLoader().getResourceAsStream("marcmap.json")
        this.marcmap = mapper.readValue(is, Map)
        is = this.getClass().getClassLoader().getResourceAsStream("thesauri.json")
        this.thesauri = mapper.readValue(is, Map)
    }

    def mapField(outjson, code, fjson, docjson) {
        if (marcref[RTYPE].mapping[code]) {
            def usecode = (marcref[RTYPE].mapping[code] instanceof Map ? code : marcref[RTYPE].mapping[code])
            marcref[RTYPE].mapping[usecode].each { property, fieldcode ->
                log.trace("Examining mapping: $property -- $fieldcode")
                if (fieldcode instanceof String) {
                    if (fieldcode.startsWith("CONSTANT:")) {
                        def v = fieldcode.substring(9)
                        outjson = Tools.insertAt(outjson, property, v)
                    } else if (!fieldcode.contains(".") || fieldcode.startsWith(usecode)) {
                        def v = getMarcValueFromField(code, fieldcode, fjson)
                        if (v) {
                            outjson = Tools.insertAt(outjson, property, v)
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
                    def match = false
                    fieldcode.each { item, fcode ->
                        def v
                        if (fcode.startsWith("CONSTANT:")) {
                            v = fcode.substring(9)
                        } else {
                            v = getMarcValueFromField(code, fcode, fjson)
                            if (v) {
                                match = true
                            }
                        }
                        if (v) {
                            obj[(item)] = v
                        }
                    }
                    if (match) {
                        outjson = Tools.insertAt(outjson, property, obj)
                    }
                }
            }

        } else if (marcref[RTYPE].process[code]) {
            def proclist = marcref[RTYPE].process[code]
            if (proclist instanceof Map) {
                proclist = [proclist]
            }
            for (proc in proclist) {
                def m = proc.level =~ /\<\<(\w+)\>\>/
                def mapping
                def params = []
                log.debug("call method ${proc.method}")
                if (m) {
                    log.debug("special proc.level: ${m[0]}")
                    def r = "${proc.method}"(outjson, code, fjson, docjson)
                    if (r) {
                        (mapping, params) = r
                    } else if (log.isTraceEnabled()) {
                        log.trace("Result from methodMapping is $r. No action taken.")
                    }
                } else {
                    log.debug("default proc.level: ${proc.level}")
                    mapping = "${proc.method}"(outjson, code, fjson, docjson)
                }
                log.debug("result mapping: $mapping")
                if (mapping) {
                    parseParamsForLevels(proc.level, params).each { level ->
                        outjson = Tools.insertAt(outjson, level, mapping)
                    }
                }
            }
        } else if (!(marcref[RTYPE].handled_elsewhere[code])) {
            outjson = dropToRaw(outjson, [(code):fjson])
        }
        //log.trace("outjson: $outjson")
        return outjson
    }


    def parseParamsForLevels(level, params) {
        def levels = (params ? [] : [level])
        params.each {
            def l = level
            it.each { var, val ->
                l = l.replaceAll("<<$var>>", val)
            }
            levels << l
        }
        return levels
    }

    Document doConvert(Document doc) {
        def injson = doc.dataAsJson
        def outjson = convertJson(injson, doc.identifier.toString())
        return new BasicDocument(doc).withData(mapper.writeValueAsBytes(outjson)).withContentType("application/ld+json")
    }

    def convertJson(injson, identifier) {
        def outjson = ["@id": identifier, "@type": "Record"]

        def ti = computeTypeInfo(injson)
        outjson.typeOfRecord = ti.typeOfRecord
        outjson.bibLevel = ti.bibLevel
        if (ti.encLevel) {
            outjson.encLevel = ti.encLevel
        }
        if (ti.catForm) {
            outjson.catForm = ti.catForm
        }
        if (ti.instanceType) {
            outjson.about = ["@type": ti.instanceType]
            if (ti.carrierType) {
                outjson.about.carrierType = ti.carrierType
            }
            if (ti.workType) {
                outjson.about.instanceOf = ["@type": ti.workType]
            }
        }

        def missing = []
        for (field in injson.fields) {
            field.each { code, fjson ->
                outjson = mapField(outjson, code, fjson, injson)
                missing.addAll(detectMissing(code, fjson))
            }
        }
        if (missing) {
            if (!outjson["unknown"]) {
                outjson["unknown"] = ["unmapped":missing as Set]
            } else {
                outjson["unknown"]["unmapped"] = missing as Set
            }
        }

        return outjson
    }

    def detectMissing(code, fjson) {
        def missing = []
        if (fjson instanceof Map) {
            fjson.subfields.each { s ->
                s.each { sc, sv ->
                    if (!convertedCodes["$code.$sc"] 
                            && !marcref.RTYPE?.purposefully_ignored?.get("$code.$sc")) {
                        log.trace("Missing $code.$sc")
                        missing << new String("$code.$sc")
                    }
                }
            }
        } else {
            if (!convertedCodes["$code."]) {
                log.trace("Missing $code")
                missing << new String("$code")
            }
        }
        return missing
    }



    // Utility methods
    Map computeTypeInfo(marcjson) {
        log.debug("leader: ${marcjson.leader}")
        def bibLevel = marcjson.leader[7]
        def typeOfRecord = marcjson.leader[6]
        def encLevel = marcjson.leader[17]
        def catForm = marcjson.leader[18]

        def ctrlField007 = getControlField("007", marcjson)
        def carrierType = ctrlField007?.charAt(0)
        def isComputerFile = carrierType == "c"
        def isOnlineResource = ctrlField007?.startsWith("cr")
        //def isOpticalDisc = ctrlField007?.startsWith("co")

        def typeInfo = [
            typeOfRecord: typeOfRecord,
            bibLevel: bibLevel,
            carrierType: carrierType,
            encLevel: encLevel,
            catForm: catForm
        ]

        // switching on bibLevel, then typeOfRecord + carrierType

        def isMonographItem = bibLevel == "m"
        if (isMonographItem) {
            if (typeOfRecord == "a" && !isComputerFile) {
                typeInfo.instanceType = "Book"
            }
            if (typeOfRecord == "i" && carrierType == "s") {
                typeInfo.instanceType = "Audiobook"
            }
            if (typeOfRecord == "a" && isOnlineResource) {
                typeInfo.instanceType = "EBook"
            }
        }

        def isSerial = bibLevel == "s"
        /*else*/ if (isSerial) {
            if (typeOfRecord == "a" && !isComputerFile) {
                typeInfo.instanceType = "Serial"
            }
            if (typeOfRecord == "a" && isOnlineResource) {
                typeInfo.instanceType = "ESerial"
            }
        }

        return typeInfo
    }

    def dropToRaw(outjson, marcjson) {
        outjson = Tools.insertAt(outjson, "unknown.fields", marcjson)
    }

    def mapDateTime(outjson, code, fjson, marcjson) {
        convertedCodes["$code."] = true
        Date.parse("yyyyMMddHHmmss.S", fjson).format("yyyy-MM-dd'T'HH:mm:ss.S")
    }

    def getControlField(field, marcjson) {
        def value
        marcjson.fields.each {
            it.each { f, v ->
                if (field == f) {
                    value = v
                }
            }
        }
        return value
    }

    def getMarcField(fieldcode, marcjson, joinChar=" ") {
        def (field, code) = fieldcode.split(/\./, 2)
        def values = []
        log.trace("field: $field, code: $code, codes: ${code.toList()}")
        marcjson.fields.each {
            it.each { f, v ->
                if (f == field) {
                    log.trace("calling getvalue at $f")
                    def mvff = getMarcValueFromField(field, code, v, joinChar)
                    if (mvff) {
                        if (mvff instanceof List) {
                            values.addAll(mvff)
                        } else {
                            values << mvff
                        }
                    }
                }
            }
        }
        log.trace("extracted values: $values")
        return values
    }

    def getMarcValueFromField(field, code, fieldjson, joinChar=" ") {
        def codes = code.toList()
        def codeValues = []
        log.trace("getMarcValueFromField: $field.$code - $fieldjson")
        if (fieldjson instanceof Map) {
            fieldjson.subfields.each { s ->
                s.each {sc, sv ->
                    if (sc in codes) {
                        this.convertedCodes["$field.$sc"] = true
                        codeValues << sv
                    }
                }
            }
        } else {
            return fieldjson
        }
        log.trace("codeValues: $codeValues")
        if (!codeValues.size()) {
            return null
        } else if (codeValues.size() == 1) {
            return codeValues[0]
        } else {
            return codeValues
        }
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
        def name = getMarcValueFromField(code, "a", fjson)
        def numerations = getMarcValueFromField(code, "b", fjson)
        def titles = getMarcValueFromField(code, "c", fjson)
        def dates = getMarcValueFromField(code, "d", fjson)
        if (name && name instanceof String) {
            person["authoritativeName"] = name.replaceAll(/,$/, "").trim()
            person["authorizedAccessPoint"] = name.replaceAll(/,$/, "").trim()
        } else if (name && name instanceof ArrayList) {
              name.each {
                  person["authoritativeName"] = it.replaceAll(/,$/, "").trim()
                  person["authorizedAccessPoint"] = it.replaceAll(/,$/, "").trim()
              }
         }

        if (numerations) {
            if (numerations instanceof String) {
                numerations = [numerations]
            }
            for (numeration in numerations) {
                numeration = numeration.replaceAll(/,$/, "")?.trim()
                if (person["authoritativeName"]) {
                    person["authoritativeName"] = person["authoritativeName"] + " " + numeration
                }
                person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + " " + numeration
                person["numeration"] = numeration
            }
        }
        if (titles) {
            if (titles instanceof String) {
                titles = [titles]
            }
            for (title in titles) {
                title = title.replaceAll(/,$/, "")?.trim()
                if (person["authoritativeName"]) {
                    person["authoritativeName"] = person["authoritativeName"] + ", " + title
                }
                person["titlesAndOtherWordsAssociatedWithName"] = title
                person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + ", " + title
            }
        }
        if (dates) {
            if (dates instanceof List) {
                dates = dates[0]
            }
            person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + ", " + dates
            dates = dates.split(/-/)
            if (dates.size() > 1) {
                person["authorizedAccessPoint"] = person["authorizedAccessPoint"] + "."
            }
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
        log.trace("person: $person")
        return person
    }

}
