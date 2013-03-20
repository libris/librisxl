package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

@Log
class MarcBib2JsonLDConverter extends BasicMarc2JsonLDConverter {
    MarcBib2JsonLDConverter(String rt) {
        super(rt)
    }

    @Override
    def mapPerson(outjson, code, fjson, marcjson) {
        def person = super.mapPerson(outjson, code, fjson, marcjson)
        def section
        if (fjson.ind2.trim()) {
            if (fjson.ind2 == "7") {
                person["source"] = getMarcValueFromField(code, "2", fjson)
            } else if (fjson.ind2 != "4") {
                person["source"] = thesauri[fjson.ind2]
            }
        } else {
            section = mapSection(code, fjson)
        }
        if (section) {
            return [person, section]
        } else {
            return person
        }
    }

    def mapSection(code, fjson) {
        def section
        def relcode = getMarcValueFromField(code, "4", fjson)
        if (relcode) {
            section = []
            relcode.split().each {
                section << ["relator":(marcref.bib.relators[it] ?: "creator")]
            }
        } else {
            section = [["relator": "authorList"]]
        }
        return section
    }

    def mapInstitution(outjson, code, fjson, marcjson) {
        def inst = mapPerson(outjson, code, fjson, marcjson)
        log.debug("inst: $inst")
        inst[0]["@type"] = "Organization"
        inst[0].remove("source")
        return inst
    }

    def mapIsbn(outjson, code, fjson, marcjson) {
        return getMarcValueFromField(code, "a", fjson).split(/\s+/, 2)[0].replaceAll("-", "")
    }

    def detectIdentifierScheme(fjson) {
        def selectedLabel
        switch((fjson["ind1"] as String)) {
            case "0":
                selectedLabel = "isrc"
                break;
            case "1":
                selectedLabel = "upc"
                break;
            case "2":
                selectedLabel = "ismn"
                break;
            case "3":
                selectedLabel = "ean"
                break;
            case "4":
                selectedLabel = "sici"
                break;
            case "8":
                selectedLabel = "unspecifiedStandardIdentifier"
                break;
        }
        return selectedLabel
    }

    def mapStandardIdentifier(outjson, code, fjson, marcjson) {
        def scheme = detectIdentifierScheme(fjson)
        def value
        if (scheme) {
            value = getMarcValueFromField(code, "a", fjson)
        }
        return [value, [["scheme":scheme]]]
    }

    def mapIdentifier(outjson, code, fjson, marcjson) {
        def scheme = (detectIdentifierScheme(fjson) ?: (getMarcValueFromField(code, "2", fjson) ?: "isbn"))
        if (scheme) {
            def otherIdentifier = ["@type":"Identifier","identifierScheme":scheme]
                if (scheme == "isbn") {
                    def iv = getMarcValueFromField(code, "a", fjson).split(/\s+/, 2)
                    otherIdentifier["identifiedValue"] = iv[0].replaceAll("-", "")
                    if (iv.length > 1) {
                        otherIdentifier["identifierNote"] = iv[1]
                    }
                    def toA = getMarcValueFromField(code, "c", fjson)
                    if (toA) {
                        otherIdentifier["termsOfAvailability"] = ["literal":toA]
                    }
                    def dep = getMarcValueFromField(code, "z", fjson)
                    if (dep) {
                        otherIdentifier["deprecatedIdentifier"] = dep
                    }
                } else {
                    otherIdentifier["identifierValue"] = getMarcValueFromField(code, "a", fjson)
                }
            return otherIdentifier
        }
        return null
    }

    def mapSubject(outjson, code, fjson, marcjson) {
        def baseurl = "http://libris.kb.se/"
        def system = "sab"
        def subjectcode = ""
        def out = [:]
        log.debug("map subject for $fjson")
        /*
        fjson["subfields"].each {
            it.each { key, value ->
                switch(key) {
                    case "a":
                    subjectcode = value
                    break;
                    case "2":
                    system = value
                    break;
                    default:
                    complete = false
                    break;
                }
            }
        }
        */
        subjectcode = getMarcValueFromField(code, "a", fjson)
        system = getMarcValueFromField(code, "2", fjson)
        if (marcref.get(RTYPE).subjects.get(system)?.containsKey(subjectcode)) {
            out = ["@id":new String(marcref.get(RTYPE).subjects[system][subjectcode])]
        }
        if (system.startsWith("kssb/")) {
            if (subjectcode =~ /\s/) {
                def (maincode, restcode) = subjectcode.split(/\s+/, 2)
                subjectcode = maincode+"/"+URLEncoder.encode(restcode)
            } else {
                if (marcref.get(RTYPE).subjects.get("sab")?.containsKey(subjectcode)) {
                    out = ["@id":new String(marcref.get(RTYPE).subjects[system][subjectcode])]
                }
                subjectcode = URLEncoder.encode(subjectcode)
            }
            out = ["@id":new String("http://libris.kb.se/sab/$subjectcode")]
        }
        log.debug("map subject out: $out")
        return out
    }

    static main(args) {
        def converter = new MarcBib2JsonLDConverter("bib")
        def mapper = converter.mapper
        def injson = new File(args[0]).withInputStream { mapper.readValue(it, Map) }
        def identifier = null
        def outjson = converter.convertJson(injson, identifier)
        println mapper.defaultPrettyPrintingWriter().
                writeValueAsString(outjson)
    }

}
