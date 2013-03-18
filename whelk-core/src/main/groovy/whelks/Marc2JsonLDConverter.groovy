package se.kb.libris.whelks.plugin

import java.text.SimpleDateFormat

import se.kb.libris.conch.Tools
import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper


@Log
class Marc2JsonLDConverter extends BasicFormatConverter implements WhelkAware, FormatConverter {
    final static String RAW_LABEL = "marc21"
    final static String UNKNOWN_LABEL = "unknown"
    final static String ABOUT_LABEL = "about"
    final static String INSTANCE_LABEL = "instanceOf"
    final static String MARCMAP_RESOURCE = "/resource/_marcmap"
    String requiredContentType = "application/json"
    String requiredFormat = "marc21"
    ObjectMapper mapper

    def marcref
    def marcmap
    Whelk whelk
    def recordType

    Marc2JsonLDConverter(def recordType) {
        this.recordType = recordType
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
        is = this.getClass().getClassLoader().getResourceAsStream("marcmap.json")
        this.marcmap = mapper.readValue(is, Map)
    }

   static main(args) {
        def uri = new URI(args[0])
        def source = new File(args[1])
        def destDir = new File(args[2])
        def dest = new File(destDir, source.name)
        def mapper = new ObjectMapper()
        log.info("source: $source")
        def inData = source.withInputStream { mapper.readValue(it, Map) }
        def converter = new Marc2JsonLDConverter(args[3])
        def outjson = converter.createJson(uri, inData)
        log.info("dest: $dest")
        dest << mapper.defaultPrettyPrintingWriter().writeValueAsString(outjson).getBytes("utf-8")
    }

    def mapDefault(String code, String value) {
        log.trace("mapDefault, string value: $code = $value")
        def marcrefValue = marcref.get(recordType).fields[code]
        def out = [:]
        if (marcrefValue) {
              if (marcrefValue instanceof Map) {
                  marcrefValue.each { k, v ->
                        if (v) {
                            out[(k)] = v
                        }
                 }
              } else {
                  out = [(marcrefValue): value]
              }
             return out
        } else if (marcrefValue == false){
            log.debug("Failed to map $code")
            return false
        }
        return null
    }

    def mapDefault(String code, Date value) {
        if (marcref.get(recordType).fields[code]) {
            def sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
            def out = [(marcref.get(recordType).fields[code]): sdf.format(value)]
            return out
        } else if (marcref.get(recordType).fields[code] == false){
            log.debug("Failed to map $code")
            return false
        }
        return null
    }

    def mapDefault(String code, Map json) {
        boolean complete = true
        def out = [:]
        log.trace("mapDefault: $code = $json")
        json.get("subfields").each {
            it.each { k, v ->
                if ((code as int) > 8) {
                    if (marcref.get(recordType).fields.get(code) == null) {
                        return null
                    }
                    def label = marcref.get(recordType).fields.get(code)?.get(k)
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
                        log.debug("Failed to map ${code}.${k}")
                        complete = false
                    }
                } else { // Map controlfield
                    log.trace("mapping controlfield $k = $v")
                    if (v.trim() && !(v =~ /^[|]+$/)) {
                        out[k] = new String("$MARCMAP_RESOURCE/$code/$k/$v")
                        /*

                        def lbl = marcmap.get(recordType).fixprops?.get(k)?.get(v)?.get("label_sv")
                        if (lbl) {
                            out[k] = ["code":v,"label":lbl,"@language":"sv"]
                        } else {
                            out[k] = v
                        }
                        */
                    }
                }
            }
        }
        if (complete) {
            log.trace("default mapping: $out")
            def objlabel = marcref.get(recordType).fields.get(code)?.get("_objectLabel")
            if (objlabel) {
                log.trace("returning nested object for $code")
                return [(objlabel):out]
            }
            return out
        }
        //return [(raw_lbl): ["fields":[(code):json]]]
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
                        out["isbn"] = value.split(/\s+/, 2)[0].replaceAll("-", "")
                        out["isbnNote"] = value.split(/\s+/, 2)[1]
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
        return false
    }

    def mapOtherIdentifierAsBNode(scheme, json) {
        try {
            def id = ["@type":"Identifier","identifierScheme":scheme]
            if (scheme == "isbn") {
                def iv = json.subfields[0]["a"].split(/\s+/, 2)
                id["identifiedValue"] = iv[0].replaceAll("-", "")
                if (iv.length > 1) {
                    id["identifierNote"] = iv[1]
                }
                log.trace("bnode other identifier: $id")
            } else {
                id["identifierValue"] = json.subfields[0]["a"]
            }
            return id
        } catch (RuntimeException e) {
            log.debug("Mapping identifier as bnode yielded exception: ${e.message}")
        }
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
                            //out["identifier"] = ["@type":"Identifier","identifierScheme":value,"identifierValue":out["_other"]]
                            //out.remove("_other")
                            out["_other_ident"] = value
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
        if ((code as int) % 100 == 0) {
            out["@type"] = "Person"
        }
        if ((code as int) % 100 == 10) {
            out["@type"] = "Organization"
        }
        if ((code as int) % 100 == 11) {
            out["@type"] = "Conference"
        }
        boolean complete = true
        json['subfields'].each {
            it.each { key, value ->
                switch (key) {
                    case "a":
                    value = value.trim().replaceAll(/,$/, "")
                    out["authoritativeName"] = value
                    def n = value.split(", ")
                    if (json["ind1"] == "1" && n.size() > 1) {
                        out["familyName"] = n[0]
                        out["givenName"] = n[1]
                        out["name"] = n[1] + " " + n[0]
                    } else {
                        out["name"] = value
                    }
                    break;
                    case "c":
                        out["title"] = value
                    break;
                    case "d":
                    def d = value.split("-")
                    out["birthYear"] = d[0]
                    if (d.length > 1) {
                        out["deathYear"] = d[1]
                    }
                    if (out["authoritativeName"]) {
                        out["authorizedAccessPoint"] = out["authoritativeName"] + ", $value"
                    }
                    break;
                    case "4":
                        break;
                    case "6":
                    //TODO: create linked resource for alternateGraphicRepresentation
                        break;
                    default:
                        complete = false
                        break;
                }
            }
        }
        if (complete) {
            return out
        } else {
            return false
        }
    }

    def mapBirthDate(code, json) {
        def out = [:]
        boolean complete = true
        json["subfields"].each {
            it.each { key, value ->
                switch (key) {
                    case "f":
                    case "g":
                        out[marcref.auth.fields[code][key][0]] = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyyMMdd").parse(value))
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


    def mapLinks(code, json) {
        def out = [:]
        boolean complete = true
        def url, source
        json["subfields"].each {
            it.each { key, value ->
                switch (key) {
                    case "u":
                        url = value
                        break
                    case "y":
                    case "z":
                        source = value
                        break
                    default:
                        complete = false
                        break
                }
            }
        }
        switch(source) {
            case "DBPedia":
                out["dbpedia"] = url
                break
            case "Wikipedia":
                out["wikipedia"] = url
                break
            case "VIAF":
                out["viaf"] = url
                break
            case "LC Name Authority":
                out["lc"] = url
                break
            case "bild":
                out["image"] = url
                break
            case "Fritt tillgänglig via":
                out["availableAt"] = url
                break
            default:
                complete = false
                break
        }
        if (complete) {
            return out
        }
        return false
    }

    def mapPublishingInfo(code, json) {
        def out = [:]
        boolean complete = true
        json["subfields"].each {
            it.each { key, value ->
                switch (key) {
                    case "a":
                        value = value.replaceAll(/:$/, "").trim()
                        out["placeOfPublication"]=["@type":"Place","label":value]
                        break
                    case "b":
                        value = value.replaceAll(/,$/, "").trim()
                        out["publisher"]=["@type":"Organization", "name":value]
                        break
                    case "c":
                        value = value.replaceAll(/;$/, "").trim()
                        out["pubDate"] = ["@type":"year","@value":value]
                        break
                    case "e":
                        value = value.replaceAll(/[()]/, "").trim()
                        out["placeOfManufacture"] = ["@type":"Place","label":value]
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

    def mapCreator(code, json) {
        def out = [:]
        boolean complete = true
        json["subfields"].each {
            it.each { key, value ->
        if (marcref.get(recordType).fields.get(code)?.containsKey(key)) {
                    marcref.get(recordType).fields[code][key].each {
                        if (key == "b") {
                            out[(it)] = value
                        } else {
                            if (!out[(it)]) {
                                out[(it)] = ["@type":"Organization", "abbr":value]
                            } else {
                                if (!(out[(it)] instanceof List)) {
                                    out[(it)] = [out[(it)]]
                                }
                                out[(it)] << ["@type":"Organization", "abbr":value]
                            }
                        }
                    }
                } else {
                    complete = false
                }
            }
        }
        if (complete) {
            return out
        }
        return false

    }


    def mapSubject(code, json) {
        def baseurl = "http://libris.kb.se/"
        def system = "sab"
        def subjectcode = ""
        def out = []
        boolean complete = true
        log.trace("map subject for $json")
        json["subfields"].each {
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
        if (!complete) {
            return false
        }
        if (marcref.get(recordType).subjects.get(system)?.containsKey(subjectcode)) {
            out << ["@id":new String(marcref.get(recordType).subjects[system][subjectcode])]
        }
        if (system.startsWith("kssb/")) {
            if (subjectcode =~ /\s/) {
                def (maincode, restcode) = subjectcode.split(/\s+/, 2)
                subjectcode = maincode+"/"+URLEncoder.encode(restcode)
            } else {
                if (marcref.get(recordType).subjects.get("sab")?.containsKey(subjectcode)) {
                    out << ["@id":new String(marcref.get(recordType).subjects[system][subjectcode])]
                }
                subjectcode = URLEncoder.encode(subjectcode)
            }
            out << ["@id":new String("http://libris.kb.se/sab/$subjectcode")]
        }
        log.trace("map subject out: $out")
        return (out.size() > 0 ? out : false)
    }

    Map dropToRaw(outjson, raw_lbl, code, json) {
        outjson = createNestedMapStructure(outjson, [raw_lbl,"fields"],[])
        outjson[raw_lbl]["fields"] << [(code):json]
        return outjson
    }

    def mapField(code, json, outjson) {
        def raw_lbl = (marcref.uninteresting.contains(code) ? RAW_LABEL : UNKNOWN_LABEL)
        if (recordType.equals("bib")) {
            switch(code) {
                case "020":
                    def i = mapIsbn(json)
                    if (i) {
                        outjson = Tools.mergeMap(outjson, [(ABOUT_LABEL):i])
                        outjson = createNestedMapStructure(outjson, [ABOUT_LABEL,"identifier"], mapOtherIdentifierAsBNode("isbn",json))
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break
                case "024":
                    def i = mapOtherIdentifier(code, json)
                    if (i) {
                        if (i.containsKey("_other")) {
                            outjson = createNestedMapStructure(outjson, [ABOUT_LABEL,"identifier"], mapOtherIdentifierAsBNode(i["_other_ident"],json))
                        } else {
                            outjson = createNestedMapStructure(outjson, [ABOUT_LABEL,"identifier"], mapOtherIdentifierAsBNode(i.find{it.key}.key,json))
                            outjson = Tools.mergeMap(outjson, [(ABOUT_LABEL):i])
                        }
                    } else {

                        outjson = createNestedMapStructure(outjson, [raw_lbl,"fields"],[])
                        outjson[raw_lbl]["fields"] << [(code):json]
                    }
                    break
                case "040":
                    def c = mapCreator(code, json)
                    if (c) {
                        outjson = Tools.mergeMap(outjson, c)
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
                case "072":
                case "084":
                    def c = mapSubject(code, json)
                    if (c) {
                        outjson = createNestedMapStructure(outjson, [ABOUT_LABEL, INSTANCE_LABEL, "subject"], [])
                        outjson[ABOUT_LABEL][INSTANCE_LABEL]["subject"].addAll(c)
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
                case "100":
                case "700":
                    def p = mapPerson(code, json)
                    if (p) {
                        def relcode = subfieldCode("4", json["subfields"])
                        log.trace("relcode: $relcode")
                        def relator = marcref[recordType].relators[relcode]
                        log.trace("mapPerson found relator $relator")
                        if (relator && relator != "author") {
                            if (relator in marcref[recordType].levels[INSTANCE_LABEL]) {
                                outjson = createNestedMapStructure(outjson, [ABOUT_LABEL, INSTANCE_LABEL, relator], [])
                                outjson[ABOUT_LABEL][INSTANCE_LABEL][relator] << p
                            } else {
                                outjson = createNestedMapStructure(outjson, [ABOUT_LABEL, relator], [])
                                outjson[ABOUT_LABEL][relator] << p
                            }
                        } else {
                            outjson = createNestedMapStructure(outjson, [ABOUT_LABEL, INSTANCE_LABEL, "authorList"], [])
                            outjson[ABOUT_LABEL][INSTANCE_LABEL]["authorList"] << p
                        }
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
                case "260":
                    def pubMapped = mapPublishingInfo(code, json)
                    if (pubMapped) {
                        outjson = Tools.mergeMap(outjson, [(ABOUT_LABEL):pubMapped])
                    } else {
                        outjson = dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
                case "856":
                    def l = mapLinks(code, json)
                    if (l) {
                        outjson = Tools.mergeMap(outjson, [(ABOUT_LABEL):l])
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break
                default:
                    log.trace("mapfield default code: $code json: ${json}}")
                    def jldMapped = mapDefault(code, json)
                    log.trace("retrieved $jldMapped")
                    if (jldMapped) {
                        if (code in marcref[recordType].levels.about) {
                            outjson = Tools.mergeMap(outjson, [(ABOUT_LABEL):jldMapped])
                        } else if (code in marcref[recordType].levels.instanceOf) {
                            outjson = Tools.mergeMap(outjson, [(ABOUT_LABEL):[(INSTANCE_LABEL):jldMapped]])
                        } else {
                            log.trace("top level merge of $jldMapped")
                            outjson = Tools.mergeMap(outjson, jldMapped)
                        }
                    } else if (jldMapped == false) {
                        return outjson
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
            }
        } else if (recordType.equals("hold")) {
            switch (code) {
                default:
                    log.trace("mapfield default code: $code json: ${json}}")
                    def jldMapped = mapDefault(code, json)
                    log.trace("retrieved $jldMapped")
                    if (jldMapped) {
                        outjson = Tools.mergeMap(outjson, jldMapped)
                    } else if (jldMapped == false) {
                        return outjson
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
            }
        } else if (recordType.equals("auth")) {
            switch (code) {
                case "046":
                    def bd = mapBirthDate(code, json)
                    if (bd) {
                        outjson = Tools.mergeMap(outjson, bd)
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break
                case "100":
                case "700":
                    def p = mapPerson(code, json)
                    if (p) {
                        outjson = Tools.mergeMap(outjson, p)
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
                case "400":
                case "500":
                    def p = mapPerson(code, json)
                    if (p) {
                        def altName = [(marcref[recordType].fields[code]): (p.authorizedAccessPoint ?: p.authoritativeName)]
                        outjson = Tools.mergeMap(outjson, altName)
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break;
                case "856":
                    def l = mapLinks(code, json)
                    if (l) {
                        outjson = Tools.mergeMap(outjson, mapLinks(code, json))
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                    break
                default:
                    log.trace("mapfield default code: $code json: ${json}}")
                    def jldMapped = mapDefault(code, json)
                    log.trace("retrieved $jldMapped")
                    if (jldMapped) {
                        outjson = Tools.mergeMap(outjson, jldMapped)
                    } else {
                        dropToRaw(outjson, raw_lbl, code, json)
                    }
                break;
            }
        }
        return outjson
    }


    Map addToOrCreateListForKey(map, obj) {
        log.trace("Got map: $map")
        log.trace("and obj: $obj")
        def key = obj.find{it.key}.key
        if (map[key]) {
            if (!(map[key] instanceof List)) {
                map[key] = [map[key]]
            }
            map[key] << obj[key]
        } else {
            map[key] = obj[key]
        }
        log.trace("return $map")
        return map
    }

    String subfieldCode(code, subfields) {
        def sfcode = null
        subfields.each {
            it.each { key, value ->
                if (key == code) {
                    sfcode = value
                }
            }
        }
        return sfcode
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
            def lastvalue = m.get(lastkey)
            if (lastvalue != null) {
                if (lastvalue instanceof List) {
                    log.trace("Last value is List")
                } else {
                    m[lastkey] = [lastvalue]
                }
                m[lastkey] << type
            } else {
                m[lastkey] = type
            }
        }

        return map
    }


    def createJson(URI identifier, Map injson) {
        def outjson = [:]
        def pfx = identifier.toString().split("/")[1]
        def marccracker = new MarcCrackerAndLabelerIndexFormatConverter(recordType)

        outjson["@context"] = "http://libris.kb.se/contexts/libris.jsonld"
        outjson["@id"] = identifier.toString()
        if (recordType.equals("bib")) {
            outjson["@type"] = "Document"
            outjson[ABOUT_LABEL] = ["@type":"Instance"]
            if (injson.containsKey("leader")) {
                injson = marccracker.rewriteJson(identifier, injson)
                    log.trace("Leader: ${injson.leader}")
                    injson.leader.subfields.each {
                        it.each { lkey, lvalue ->
                            lvalue = lvalue.trim()
                            if (lvalue && !(lvalue =~ /^\|+$/)) {
                                outjson[lkey] = new String("$MARCMAP_RESOURCE/leader/$lkey/$lvalue")
                                /*
                                def lbl = marcmap[recordType].fixprops?.get(lkey)?.get(lvalue)?.get("label_sv")
                                if (lkey == "typeOfRecord") {
                                    switch (lvalue) {
                                        case "a":
                                        outjson[ABOUT_LABEL]["@type"] << "Book"
                                        break;
                                        case "j":
                                        outjson[ABOUT_LABEL]["@type"] << "Music Recording"
                                        break;
                                    }
                                }
                                if (lkey in marcref[recordType].levels.instanceOf) {
                                    outjson = createNestedMapStructure(outjson, [ABOUT_LABEL, INSTANCE_LABEL, lkey],["code":lvalue,"label":(lbl ?: ""),"@language":"sv"])
                                } else if (lkey in marcref[recordType].levels.about) {
                                    outjson = createNestedMapStructure(outjson, [ABOUT_LABEL, lkey], ["code":lvalue,"label":(lbl ?: ""),"@language":"sv"])
                                } else {
                                    outjson[lkey] = ["code":lvalue,"label":(lbl ?: ""),"@language":"sv"]
                                }
                                */
                            }
                        }
                    }
            }
        } else if (recordType.equals("hold")) {
            if (injson.containsKey("leader")) {
                log.trace("Leader: ${injson.leader}}")
                injson = marccracker.rewriteJson(identifier, injson)
                if (injson.containsKey("leader")) {
                    log.debug("injson.leader ${injson.leader}")
                    def resourceType = "unknown"
                    def typeValue, statValue
                    injson.leader.subfields.each {
                        it.each { key, value ->
                            switch (key.trim()) {
                                case "type":
                                    typeValue = value.trim()
                                    break
                                case "statistics":
                                    statValue = value.trim()
                                    break
                            }
                            if (typeValue?.equals("x") && statValue?.matches(/[acdm]/)) {
                                resourceType = "monographic"
                            } else if (typeValue?.equals("y") && statValue?.matches(/[bis]/)) {
                                resourceType = "serial"
                            }
                        }
                    }
                    outjson["@issueMode"] = resourceType
                }
            }
        } else if (recordType.equals("auth")) {
            if (injson.containsKey("leader")) {
                injson = marccracker.rewriteJson(identifier, injson)
                log.trace("Leader: ${injson.leader}")
                log.trace("Rewritten injson ${injson.leader}")
                    injson.leader.subfields.each {
                        it.each { lkey, lvalue ->
                            lvalue = lvalue.trim()
                            if (lvalue && !(lvalue =~ /^\|+$/)) {
                                outjson[lkey] = new String("$MARCMAP_RESOURCE/leader/$lkey/$lvalue")
                            }
                        }
                    }
            }
        }

        injson.fields.each {
            it.each { fkey, fvalue ->
                log.trace("$fkey = $fvalue")
                outjson = mapField(fkey, fvalue, outjson)
            }
        }
        log.trace("Constructed JSON:\n" + mapper.defaultPrettyPrintingWriter().writeValueAsString(outjson))
        return outjson
    }

    @Override
    List<Document> doConvert(Document idoc) {
        def injson = mapper.readValue(idoc.dataAsString, Map)
        return [new BasicDocument(idoc).withData(mapper.writeValueAsBytes(createJson(idoc.identifier, injson))).withFormat("jsonld")]
    }
}
