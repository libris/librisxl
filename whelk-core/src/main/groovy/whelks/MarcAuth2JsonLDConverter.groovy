package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

@Log
class MarcAuth2JsonLDConverter extends BasicMarc2JsonLDConverter {

    MarcAuth2JsonLDConverter(String rt) {
        super(rt)
    }

    @Override
    def mapPerson(outjson, code, fjson, marcjson) {
        def person = super.mapPerson(outjson, code, fjson, marcjson)
        def altLabels = []
        for (f in ["400.a","410.a","411.a","500.a","510.a","511.a"]) {
            altLabels.addAll(getMarcField(f, marcjson))
        }
        if (altLabels) {
            log.trace("altLabels: $altLabels")
            person["label"] = altLabels
        }
        def bdf = getMarcField("046.f", marcjson)
        def ddf = getMarcField("046.g", marcjson)
        if (bdf) {
            person["birthDate"] = Date.parse("yyyyMMdd", bdf[0]).format("yyyy-MM-dd")
        }
        if (ddf) {
            person["deathDate"] = Date.parse("yyyyMMdd", ddf[0]).format("yyyy-MM-dd")
        }

        return person
    }

    def mapTerm(outjson, code, fjson, marcjson) {
        def term = [:]
        term["term"] = getMarcValueFromField("a", fjson)
        term["number"] = getMarcValueFromField("0", fjson)
        if (fjson.ind2.trim()) {
            if (fjson.ind2 == "7") {
                term["source"] = getMarcValueFromField("2", fjson)
            } else if (fjson.ind2 != "4") {
                term["source"] = thesauri[fjson.ind2]
            }
        } 
        return term
    }
}
