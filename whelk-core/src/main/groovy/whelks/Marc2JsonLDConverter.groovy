package se.kb.libris.whelks.plugin

import java.text.SimpleDateFormat

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper


@Log
class Marc2JsonLDConverter extends BasicPlugin implements FormatConverter {
    final static String RAW_LABEL = "marc21"
    String requiredContentType = "application/json"
    String requiredFormat = "marc21"
    ObjectMapper mapper

    def marcref
    def marcmap

    Marc2JsonLDConverter() {
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
        is = this.getClass().getClassLoader().getResourceAsStream("marcmap.json")
        this.marcmap = mapper.readValue(is, Map)
    }

   static main(args) {
        def uri = new URI(args[0])
        def injson = args[1]
        def mapper = new ObjectMapper()
        def converter = new Marc2JsonLDConverter()
        log.info("file: marc2jsonld/in/$injson")
        InputStream is = converter.getClass().getClassLoader().getResourceAsStream("marc2jsonld/in/"+injson )
        def infile = mapper.readValue(is, Map)
        def outjson = converter.createJson(uri, infile)
        def file = new File("src/main/resources/marc2jsonld/ut/$injson")
        file << mapper.defaultPrettyPrintingWriter().writeValueAsString(outjson).getBytes("utf-8")
    }

    def mapDefault(String code, String value) {
        if (marcref.fields[code]) {
            def out = [(marcref.fields[code]): value]
            return out
        } else {
            return false
        }
    }

    def mapDefault(String code, Date value) {
        if (marcref.fields[code]) {
            def sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
            def out = [(marcref.fields[code]): sdf.format(value)]
            return out
        } else {
            return false
        }
    }

    def mapDefault(String code, Map json) {
        boolean complete = true
        def out = [:]
        log.trace("mapDefault: $code = $json")
        json.get("subfields").each {
            it.each { k, v ->
                def label = marcref.fields.get(code)?.get(k)
                if (label instanceof Map) {
                    assignValue(out, label, v)
                } else if (label instanceof List) {
                    label.each {
                        if (out.containsKey(it)) {
                            if (!(out[(it)] instanceof List)) {
                                def cval = out[(it)]
                                out[(it)] = [cval]
                            }
                            out[(it)] << v
                        } else {
                            out[(it)] = v
                        }
                    }
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
        //return [(RAW_LABEL): ["fields":[(code):json]]]
        return false
    }

    private void assignValue(Map out, Map refmap, def value) {
        refmap.each { rk, rv ->
            if (!(rv instanceof Map)) {
                if (out[(rk)]) {
                    out[(rk)] << [(rv):value]
                } else {
                    out[(rk)] = [(rv):value]
                }
            } else {
                if (out.containsKey(rk)) {
                    if (!(out.get(rk) instanceof Map)) {
                        def ov = out.get(rk)
                        out[(rk)] = ["value":ov]
                    }
                } else {
                    out[(rk)] = [:]
                }
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
                        out["isbnData"] = value.split(" ")[1]
                    } else {
                        out["isbn"] = value.replaceAll("-", "")
                    }
                    break;
                case "c":
                    log.trace("isbn c: $value")
                    out["termsOfAvailability"]=["literal":value]
                    break;
                case "z":
                    log.trace("isbn z: $value")
                    out["deprecatedIsbn"]=value
                    break;
                default:
                    log.trace("isbn unknown: $key == $value")
                    complete = false
                    break;
            }
        } }
        if (complete) {
            return out
        }
        //return [(RAW_LABEL):["fields":["020":json]]]
        return false
    }

    def mapOtherIdentifier(code, json) {
        def out = [:]
        boolean complete = true
        def selectedLabel
        log.trace("ind1: ${json.ind1}")
        switch((json["ind1"] as String)) {
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
            case "7":
                selectedLabel = "_other"
                break;
            case "8":
                selectedLabel = "unspecifiedStandardIdentifier"
                break;
        }
        log.trace("selectedLabel : $selectedLabel")
        if (!selectedLabel) {
            log.trace("Unable to find a label for ${json.ind1}")
            complete = false
        } else {
            json["subfields"].each {
                it.each { key, value ->
                    switch (key) {
                        case "a":
                            out[selectedLabel] = value
                            break
                        case "2":
                            out["identifier"] = ["@type":"Identifier","identifierScheme":value,"identifierValue":out["_other"]]
                            out.remove("_other")
                            break;
                        default:
                            log.trace("No rule for key $key")
                            complete = false
                            break
                    }
                }
            }
        }
        if (complete) {
            log.trace("mapped other identifier: $out")
            return out
        }
        return false
    }

    def mapPerson(code, json) {
        def out = [:]
        boolean complete = true
        def creatorLabel = "author"
        log.trace("subfields: " + json['subfields'])
        def person = [:]
        json['subfields'].each {
            log.trace("subfield: $it")
            it.each { key, value ->
                switch (key) {
                    case "a":
                    value = value.trim().replaceAll(/,$/, "")
                    person["preferredNameForThePerson"] = value
                    def n = value.split(", ")
                    if (json["ind1"] == "1" && n.size() > 1) {
                        person["surname"] = n[0]
                        person["givenName"] = n[1]
                        person["name"] = n[1] + " " + n[0]
                    } else {
                        person["name"] = value
                    }
                    break;
                    case "d":
                    def d = value.split("-")
                    person["dateOfBirth"] = ["@type":"year", "@value": d[0]]
                    if (d.length > 1) {
                        person["dateOfDeath"] = ["@type":"year", "@value": d[1]]
                    }
                    break;
                    case "4":
                        if (marcref.relators[value]) {
                            creatorLabel = marcref.relators[value]
                        } else {
                            complete = false
                        }
                        break;
                    default:
                        complete = false
                        break;
                }
            }
        }
        if (complete) {
            log.trace("Adding $person to $creatorLabel")
            out[(creatorLabel)] = person
            return out
        } else {
            return false
        }
    }

    def mapPublishingInfo(code, json) {
        def out = [:]
        boolean complete = true
        json["subfields"].each {
            it.each { key, value ->
                switch (key) {
                    case "a":
                        value = value.replaceAll(/:$/, "").trim()
                        out["placeOfPublication"]=["label":value]
                        break
                    case "b":
                        value = value.replaceAll(/,$/, "").trim()
                        out["publisherName"]=value
                        break
                    case "c":
                        value = value.replaceAll(/;$/, "").trim()
                        out["dateOfPublication"] = ["@type":"year","@value":value]
                        break
                    case "e":
                        value = value.replaceAll(/[()]/, "").trim()
                        out["placeOfManufacture"] = ["label":value]
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
        return false
    }

    // TODO: Break out the switch
    def mapCreator(code, json) {
        def out = [:]
        boolean complete = true
        json["subfields"].each {
            it.each { key, value ->
                switch (key) {
                    case "a":
                        out["creator"]=["@type":"foaf:Organization", "abbr":value]
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
        return false

    }

    def mapField(code, json, outjson) {
        switch(code) {
            case "020":
                def i = mapIsbn(json)
                if (i) {
                    def isbn = ["describes":i]
                    outjson = mergeMap(outjson, isbn)
                } else {
                    outjson = createNestedMapStructure(outjson, [RAW_LABEL,"fields"],[])
                    outjson[RAW_LABEL]["fields"] << [(code):json]
                }
                break
            case "024":
                def i = mapOtherIdentifier(code, json)
                if (i) {
                    outjson = mergeMap(outjson, ["describes":i])
                } else {
                    outjson = createNestedMapStructure(outjson, [RAW_LABEL,"fields"],[])
                    outjson[RAW_LABEL]["fields"] << [(code):json]
                }
                break
            case "040":
                def c = mapCreator(code, json)
                if (c) {
                    outjson = mergeMap(outjson, c)
                } else {
                    outjson = createNestedMapStructure(outjson, [RAW_LABEL,"fields"],[])
                    outjson[RAW_LABEL]["fields"] << [(code):json]
                }
                break;
            case "100":
            case "700":
                def p = mapPerson(code, json)
                if (p) {
                    outjson = createNestedMapStructure(outjson, ["describes", "expression", "creators"], [])
                    outjson["describes"]["expression"]["creators"] << p
                } else {
                    outjson = createNestedMapStructure(outjson, [RAW_LABEL,"fields"],[])
                    outjson[RAW_LABEL]["fields"] << [(code):json]
                }
                break;
            case "260":
                def pubMapped = mapPublishingInfo(code, json)
                if (pubMapped) {
                    outjson = mergeMap(outjson, pubMapped)
                } else {
                    outjson = createNestedMapStructure(outjson, [RAW_LABEL,"fields"],[])
                    outjson[RAW_LABEL]["fields"] << [(code):json]
                }
                break;
            default:
                def jldMapped = mapDefault(code, json)
                log.trace("retrieved $jldMapped")
                if (jldMapped) {
                    if (code in marcref.levels.describes) {
                        outjson = mergeMap(outjson, ["describes":jldMapped])
                    } else if (code in marcref.levels.expression) {
                        outjson = mergeMap(outjson, ["describes":["expression":jldMapped]])
                    } else {
                        log.trace("top level merge of $jldMapped")
                        outjson = mergeMap(outjson, jldMapped)
                    }
                } else {
                    outjson = createNestedMapStructure(outjson, [RAW_LABEL,"fields"],[])
                    outjson[RAW_LABEL]["fields"] << [(code):json]
                }
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
                    log.trace("$key not found. creating new map.")
                    m.put(key, [:])
                    m = m.get(key)
                } else if (m[(key)] instanceof Map) {
                    m = m.get(key)
                }
            }
        }
        def lastkey = keys.get(keys.size()-1)
        log.trace("lastkey: $lastkey, m[lastkey]: " + m.get(lastkey))
        if (m.get(lastkey) != null && type instanceof List) {
            log.trace("Last item is List")
            def v = m[(keys[keys.size()-1])]
            if (!(v instanceof List)) {
                m[(keys[keys.size()-1])] = [v]
            }
        } else {
            m[(keys[keys.size()-1])] = type
        }

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
        def marccracker = new MarcCrackerAndLabelerIndexFormatConverter()

        outjson["@context"] = "http://libris.kb.se/contexts/libris.jsonld"
        outjson["@id"] = identifier.toString()
        outjson["@type"] = "Record"
        if (injson.containsKey("leader")) {
            injson = marccracker.rewriteJson(identifier, injson)
            log.trace("Leader: ${injson.leader}")
            injson.leader.subfields.each { 
                it.each { lkey, lvalue ->
                    lvalue = lvalue.trim()
                    if (lvalue && !(lvalue =~ /^\|+$/)) {
                        def lbl = marcmap.bib.fixprops?.get(lkey)?.get(lvalue)?.get("label_sv")
                        if (lbl) {
                            outjson[lkey] = ["code":lvalue,"label":lbl]
                        } else {
                            outjson[lkey] = lvalue
                        }
                    }
                }
            }
        }
        injson.fields.each {
            log.trace("Working on json field $it")
            it.each { fkey, fvalue ->
                outjson = mapField(fkey, fvalue, outjson)
                log.trace("outjson: $outjson")
            }
        }
        log.trace("Constructed JSON:\n" + mapper.defaultPrettyPrintingWriter().writeValueAsString(outjson))
        return outjson
    }

    @Override
    List<Document> convert(Document idoc) {
        def outdocs = []
        if (idoc.contentType == this.requiredContentType && idoc.format == this.requiredFormat) {
            def injson = mapper.readValue(idoc.dataAsString, Map)
            outdocs << new BasicDocument(idoc).withData(mapper.writeValueAsBytes(createJson(idoc.identifier, injson))).withFormat("jsonld")
        } else {
            log.warn("This converter requires $requiredFormat in $requiredContentType. Document ${idoc.identifier} is ${idoc.format} in ${idoc.contentType}")
        }
        return outdocs
    }

    @Override
    List<Document> convert(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            outdocs.addAll(convert(doc))
        }
        return outdocs
    }
}
