package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

@Log
class MarcBib2JsonLDConverter extends BasicMarc2JsonLDConverter {
    def thesauri
    MarcBib2JsonLDConverter(String rt) {
        super(rt)
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("thesauri.json")
        this.thesauri = mapper.readValue(is, Map)

    }

    @Override
    def mapPerson(outjson, code, fjson, marcjson) {
        def person = super.mapPerson(outjson, code, fjson, marcjson)
        def section
        if (fjson.ind2.trim()) {
            if (fjson.ind2 == "7") {
                person["source"] = getMarcValueFromField("2", fjson)
            } else if (fjson.ind2 != "4") {
                person["source"] = thesauri[fjson.ind2]
            }
        } else {
            section = ["relator": "authorList"]
        }
        if (section) {
            return [person, section]
        } else {
            return person
        }
    }

    def mapIsbn(outjson, code, fjson, marcjson) {
        return getMarcValueFromField("a", fjson).split(/\s+/, 2)[0].replaceAll("-", "")
    }

    def mapIdentifier(outjson, code, fjson, marcjson) {
        def scheme = "isbn"
        def id = ["@type":"Identifier","identifierScheme":scheme]
        if (scheme == "isbn") {
            def iv = fjson.subfields[0]["a"].split(/\s+/, 2)
            id["identifiedValue"] = iv[0].replaceAll("-", "")
            if (iv.length > 1) {
                id["identifierNote"] = iv[1]
            }
            log.trace("bnode other identifier: $id")
        } else {
            id["identifierValue"] = fjson.subfields[0]["a"]
        }
        return id
    }

    def mapSubject(outjson, code, fjson, marcjson) {
        def baseurl = "http://libris.kb.se/"
        def system = "sab"
        def subjectcode = ""
        def out = [:]
        boolean complete = true
        log.debug("map subject for $fjson")
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
}

