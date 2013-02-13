package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper


@Log
class Marc2JsonLDConverter extends MarcCrackerAndLabelerIndexFormatConverter implements FormatConverter {

    String requiredContentType = "application/json"
    final static String RAW_LABEL = "marc21"
    def marcref

    Marc2JsonLDConverter() {
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
    }

    Map mapDefault(String code, def value) {
        if (marcref[code]) {
            return [(marcref[code]): value]
        } else {
            return [(RAW_LABEL) : [(code): value]]
        }
    }

    Map mapDefault(String code, Map json) {
        boolean complete = true
        def out = [:]
        log.trace("mapDefault: $code = $json")
        json.get("subfields").each {
            it.each { k, v ->
                def label = marcref?.get(code)?.get(k)
                log.trace("label: $label")
                if (label instanceof Map) {
                    assignValue(out, label, v)
                } else if (label) {
                    out[label] = v
                } else if (label == null) {
                    complete = false
                }
            }
        }
        if (complete) {
            log.trace("default mapping: $out")
            return out
        }
        return [(RAW_LABEL): [(code):json]]
    }

    private void assignValue(Map out, Map refmap, def value) {
        refmap.each { rk, rv ->
            log.trace("rk: $rk, rv: $rv")
            if (!(rv instanceof Map)) {
                log.trace("$rv is not map. Setting $rk = [$rv : $value]")
                if (out[(rk)]) {
                    out[(rk)] << [(rv):value]
                } else {
                    out[(rk)] = [(rv):value]
                }
            } else {
                if (out.containsKey(rk)) {
                    log.trace("out already contains an $rk")
                    if (!(out.get(rk) instanceof Map)) {
                        log.trace(" ... and it's not a Map!")
                        def ov = out.get(rk)
                        out[(rk)] = ["value":ov]
                    }
                } else {
                    log.trace("no previous key named $rk")
                    out[(rk)] = [:]
                }
                log.trace("creating map at $out")
                assignValue(out[(rk)], rv, value)
            }
        }
    }

    def mapIsbn(json) {
        def out = [:]
        boolean complete = true
        json['subfields'].each { it.each { key, value ->
            switch (key) {
                case "a":
                    log.trace("isbn a: $value")
                    if (value.contains(" ")) {
                        out["isbn"] = value.split(" ")[0].replaceAll("-", "")
                        out["isbnRemainder"] = value.split(" ")[1]
                    } else {
                        out["isbn"] = value.replaceAll("-", "")
                    }
                    break;
                case "c":
                    log.trace("isbn c: $value")
                    out["termsOfAvailability"]=["literal":value]
                    break;
                default:
                    log.trace("isbn unknown: $key == $value")
                    complete = false
                    break;
            }
        } }
        if (complete) {
            return ["describes":out]
        }
        return [(RAW_LABEL):["020":json]]
    }

    def mapIdentifier(code, json) {
        def out = [:]
        boolean complete = true
        json["subfields"].each {
            it.each { key, value ->
                switch (key) {
                    case "a":
                        out["identifierForTheManifestation"] = value
                        out["isbn"] = value.replaceAll("[^\\d]", "")
                        break
                    case "c":
                        break
                    default:
                        complete = false
                        break
                }
            }
        }
        if (complete) {
            return out
        }
        return ["raw": [(code):json]]
    }

    def mapPerson(code, json) {
        def out = [:]
        boolean complete = true
        log.trace("subfields: " + json['subfields'])
        json['subfields'].each { 
            log.trace("IT: $it")
            it.each { key, value ->
                log.trace("key: $key, value: $value")
            switch (key) {
                case "a":
                    out["preferredNameForThePerson"] = value
                    if (json["ind1"] == "1") {
                        def n = value.split(", ")
                        out["surname"] = n[0]
                        out["givenName"] = n[1]
                        out["name"] = n[1] + " " + n[0]
                    } else {
                        out["name"] = value
                    }
                    break;
                case "d":
                    def d = value.split("-")
                    out["dateOfBirth"] = ["@type":"year", "@value": d[0]]
                    if (d.length > 1) {
                        out["dateOfDeath"] = ["@type":"year", "@value": d[1]]
                    }
                    break;
                default:
                    complete = false
                    break;
            }
        } }
        if (complete) {
            /*
            def rout = ["describes":["expressionManifested":["authorList": []]]]
            rout["describes"]["expressionManifested"]["authorList"] << out
            return rout
            */
            return out
        } else {
            return [(RAW_LABEL): [(code):json]]
        }
    }

    def mapField(code, json, outjson) {
        switch(code) {
            case "020":
                outjson = mergeMap(outjson, mapIsbn(json))
                log.trace("injson: $json")
                //outjson = mergeMap(outjson, mapIdentifier(code, json))
                break
            case "100":
            case "700":
                outjson = createNestedMapStructure(outjson, ["describes", "expressionManifested", "authorList"], [])
                outjson["describes"]["expressionManifested"]["authorList"] <<  mapPerson(code, json)
                break;
            default:
                outjson = mergeMap(outjson, mapDefault(code, json))
                log.trace("OutJson now: $outjson")
                break;
        }
        return outjson
    }

    Map createNestedMapStructure(map, keys, type) {
        def m = map
        keys.eachWithIndex() { key, i ->
            if (i < keys.size()-1) {
                if (!m.containsKey(key)) {
                    m.put(key, [:])
                    m = m.get(key)
                } else if (m[(key)] instanceof Map) {
                    log.trace("Hey! It's a map!")
                    m = m.get(key)
                }
            }
        }
        log.trace("this is map after loop: $m")
        if (m[(keys[keys.size()-1])] != null && type instanceof List) {
            def v = m[(keys[keys.size()-1])]
            if (!(v instanceof List)) {
                m[(keys[keys.size()-1])] = [v]
            }
        } else {
            m[(keys[keys.size()-1])] = type
        }
        log.trace("this is map after assignment: $map")

        return map
    }

    Map mergeMap(Map origmap, Map newmap) {
        newmap.each { key, value -> 
            if (origmap.containsKey(key)) { // Update value for original map
                if (value instanceof Map && origmap.get(key) instanceof Map) {
                    origmap[key] = mergeMap(origmap.get(key), value)
                } else {
                    if (!(origmap.get(key) instanceof List)) {
                        log.trace("creating list at $key")
                        origmap[key] = [origmap[key]]
                    }
                    log.trace("adding to list at $key")
                    origmap[key] << value
                }
            } else { // Add key to original map
                origmap[key] = value
            }
        }
        log.trace("updated origmap: $origmap")
        return origmap
    }


    def createJson(URI identifier, Map injson) {
        def outjson = [:]
        def pfx = identifier.toString().split("/")[1]
        outjson["@context"] = "http://libris.kb.se/contexts/libris.jsonld"
        outjson["@id"] = identifier.toString()
        if (injson.containsKey("leader")) {
            injson = rewriteJson(identifier, injson)
                log.trace("Leader: ${injson.leader}")
                injson.leader.subfields.each { 
                    it.each { lkey, lvalue ->
                        lvalue = lvalue.trim()
                        if (lvalue && !(lvalue =~ /^\|+$/)) {
                            outjson[lkey] = lvalue
                        }
                    }
                }
        }
        injson.fields.each {
            log.trace("Working on json field $it")
            it.each { fkey, fvalue ->
                outjson = mapField(fkey, fvalue, outjson)
                log.trace("outjson: $outjson")
                /*
                if ((fkey as int) > 5 && (fkey as int) < 9) {
                    fvalue["subfields"].each {
                        it.each { skey, svalue ->
                            svalue = svalue.trim()
                            if (svalue && !(svalue =~ /^\|+$/)) {
                                outjson[skey] = svalue
                            }
                        }
                    }
                } else {
                    log.trace("Value: $fvalue")
                    if (marcref[pfx][fkey]) {
                        log.trace("Found a reference: " +marcref[pfx][fkey])
                        fvalue["subfields"].each {
                            it.each { skey, svalue ->
                                def label  = marcref[pfx][fkey][skey]
                                def linked = marcref[pfx][fkey]["_linked"]
                                if (linked) {
                                    log.trace("Create new entity for $fkey")
                                    createEntity(fvalue)
                                } else if (label) {
                                    if (outjson[label]) {
                                        log.trace("Adding $svalue to outjson")
                                        if (outjson[label] instanceof List) {
                                            outjson[label] << svalue
                                        } else {
                                            def l = []
                                            l << outjson[label]
                                            l << svalue
                                            outjson[label] = l
                                        }
                                    } else {
                                        log.trace("Inserting $svalue in outjson")
                                        outjson[label] = svalue
                                    }
                                }
                            }
                        }
                    }
                }
                */
            }
        }
        return outjson
    }

    /*
    def createJson(URI identifier, Map injson) {
        def outjson = [:]
        def pfx = identifier.toString().split("/")[1]
        outjson["@context"] = "http://libris.kb.se/contexts/libris.jsonld"
        outjson["@id"] = identifier.toString()
        // Workaround to prevent original data from being changed
        //outjson["marc21"] = mapper.readValue(mapper.writeValueAsBytes(injson), Map)
        injson = rewriteJson(identifier, injson)
        log.trace("Leader: ${injson.leader}")
        injson.leader.subfields.each { 
            it.each { lkey, lvalue ->
                lvalue = lvalue.trim()
                if (lvalue && !(lvalue =~ /^\|+$/)) {
                    outjson[lkey] = lvalue
                }
            }
        }
        injson.fields.each {
            log.trace("Working on json field $it")
            it.each { fkey, fvalue ->
                if ((fkey as int) > 5 && (fkey as int) < 9) {
                    fvalue["subfields"].each {
                        it.each { skey, svalue ->
                            svalue = svalue.trim()
                            if (svalue && !(svalue =~ /^\|+$/)) {
                                outjson[skey] = svalue
                            }
                        }
                    }
                } else {
                    log.trace("Value: $fvalue")
                    if (marcref[pfx][fkey]) {
                        log.trace("Found a reference: " +marcref[pfx][fkey])
                        fvalue["subfields"].each {
                            it.each { skey, svalue ->
                                def label  = marcref[pfx][fkey][skey]
                                def linked = marcref[pfx][fkey]["_linked"]
                                if (linked) {
                                    log.trace("Create new entity for $fkey")
                                    createEntity(fvalue)
                                } else if (label) {
                                    if (outjson[label]) {
                                        log.trace("Adding $svalue to outjson")
                                        if (outjson[label] instanceof List) {
                                            outjson[label] << svalue
                                        } else {
                                            def l = []
                                            l << outjson[label]
                                            l << svalue
                                            outjson[label] = l
                                        }
                                    } else {
                                        log.trace("Inserting $svalue in outjson")
                                        outjson[label] = svalue
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return outjson
    }
    */

    def createEntity(data) {
    }


    @Override
    List<Document> convert(Document idoc) {
        outdocs = []
        if (doc.contentType == this.requiredContentType) {
            def injson = mapper.readValue(doc.dataAsString, Map)
            outdocs << new BasicDocument(doc).withData(mapper.writeValueAsBytes(createJson(doc.identifier, injson)))
        } else {
            log.warn("This converter requires $requiredContentType. Document ${doc.identifier} is ${doc.contentType}")
        }
        return outdocs
    }

    @Override
    List<Document> convert(List<Document> docs) {
        outdocs = []
        for (doc in docs) {
            outdocs << convert(doc)
        }
        return outdocs
    }
}
