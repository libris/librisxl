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
        return person
    }

}
