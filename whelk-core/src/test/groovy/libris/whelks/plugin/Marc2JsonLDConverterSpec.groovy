package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.Whelk
import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class Marc2JsonLDConverterSpec extends Specification implements Marc2JsonConstants {

    def mapper = new ObjectMapper()
    def whelk = Mock(Whelk) {
        getPrefix() >> "bib"
    }
    def conv = new Marc2JsonLDConverter()

    def "should map document"() {
        given:
            conv.setWhelk(whelk)
        expect:
            conv.createJson(new URI(uri), loadJson(injson)) == loadJson(outjson)
        where:
            uri                     | injson                      | outjson
            /*
            "/bib/8261338"          | "in/bib/8261338.json"       | "expected/bib/8261338.json"
            "/bib/12035894"         | "in/bib/12035894.json"      | "expected/bib/12035894.json"
            "/bib/12732969"         | "in/bib/12732969.json"      | "expected/bib/12732969.json"
            */
            "/bib/12384646"         | "in/bib/12384646.json"      | "expected/bib/12384646.json"
            "/bib/7149593"          | "in/bib/7149593.json"       | "expected/bib/7149593.json"
    }

    def "should map author"() {
        expect:
            conv.mapPerson("100", marc) == jsonld
        where:
            marc          | jsonld
            AUTHOR_MARC_0 | AUTHOR_LD_0
            AUTHOR_MARC_1 | AUTHOR_LD_1
            AUTHOR_MARC_2 | AUTHOR_LD_2
            AUTHOR_MARC_3 | AUTHOR_LD_3
            AUTHOR_MARC_4 | AUTHOR_LD_4
    }

    def "should map author from document"() {
        given:
            conv.setWhelk(whelk)
        expect:
            conv.createJson(new URI(uri), loadJson(injson))["about"]["instanceOf"]["authorList"] == loadJson(outjson)["about"]["instanceOf"]["authorList"]
        where:
            uri              | injson                 | outjson
            "/bib/7149593"   | "in/bib/7149593.json"  | "expected/bib/7149593.json"
    }

    def "should map plain about for Tove"() {
        given:
            conv.setWhelk(whelk)
        expect:
            conv.createJson(new URI(uri), loadJson(injson))["about"].remove("instanceOf") == loadJson(outjson)["about"].remove("instanceOf")
        where:
            uri              | injson                 | outjson
            "/bib/7149593"   | "in/bib/7149593.json"  | "expected/bib/7149593.json"
    }

    def "should map multiple authors"() {
        given:
            conv.setWhelk(whelk)
        expect:
            conv.createJson(new URI("/bib/1234"), marc)["about"]["instanceOf"]["authorList"] == jsonld
        where:
            marc                | jsonld
             AUTHOR_MULT_MARC_0 | AUTHOR_MULT_LD_0
             AUTHOR_MULT_MARC_1 | AUTHOR_MULT_LD_1
    }

    def "should map illustrator"() {
        given:
            conv.setWhelk(whelk)
        expect:
            conv.createJson(new URI("/bib/1234"), marc)["about"]["illustrator"] == jsonld
        where:
            marc                | jsonld
             AUTHOR_MULT_MARC_1 | AUTHOR_MULT_LD_2
    }

    def "defaultMap exerciser"() {
        expect:
            conv.mapDefault(code, marc) == jsonld
        where:
            code  | marc                | jsonld
            "042" | BIBLIOGRAPHY_MARC_0 | BIBLIOGRAPHY_LD_0
    }

    def "should map controlfields"() {
        expect:
            conv.mapDefault(code, marc) == jsonld
        where:
            code  | marc             | jsonld
            "001" | CTRLNR_MARC_0    | CTRLNR_LD_0
            "005" | TIMESTAMP_MARC_0 | TIMESTAMP_LD_0

    }

    def "should map title"() {
        expect:
            conv.mapDefault("245", marc) == jsonld
        where:
            marc            | jsonld
            TITLE_MARC_0    | TITLE_LD_0
            TITLE_MARC_1    | TITLE_LD_1
    }

    def "should map publisher"() {
        expect:
            conv.mapPublishingInfo("260", marc) == jsonld
        where:
            marc             | jsonld
            PUBLISHER_MARC_0 | PUBLISHER_LD_0
    }

    def "should map publisher from document"() {
        given:
            conv.setWhelk(whelk)
        expect:
            conv.createJson(new URI(uri), loadJson(injson))["about"]["placeOfPublication"] == loadJson(outjson)["about"]["placeOfPublication"]
            conv.createJson(new URI(uri), loadJson(injson))["about"]["placeOfManufacture"] == loadJson(outjson)["about"]["placeOfManufacture"]
            conv.createJson(new URI(uri), loadJson(injson))["about"]["publisher"] == loadJson(outjson)["about"]["publisher"]
            conv.createJson(new URI(uri), loadJson(injson))["about"]["pubDate"] == loadJson(outjson)["about"]["pubDate"]
        where:
            uri              | injson                 | outjson
            "/bib/7149593"   | "in/bib/7149593.json"  | "expected/bib/7149593.json"
    }

    def "should map isbn"() {
        expect:
            conv.mapIsbn(marc) == jsonld
        where:
            marc          | jsonld
            ISBN_MARC_0   | ISBN_LD_0
            ISBN_MARC_1   | ISBN_LD_1
            ISBN_MARC_2   | ISBN_LD_2
            ISBN_MARC_3   | ISBN_LD_3
    }

    def "should map other identifier"() {
        log.info("### should map other identifier")
        expect:
            conv.mapOtherIdentifier("024", marc) == jsonld
        where:
            marc                | jsonld
            OTH_IDENT_MARC_1    | OTH_IDENT_LD_1
            OTH_IDENT_MARC_2    | OTH_IDENT_LD_2
    }

    def "should merge maps"() {
        expect:
            conv.mergeMap(o, n) == m
        where:
            o                              | n                                | m
            ["foo":"bar"]                  | ["foo":"beer"]                   | ["foo": ["bar", "beer"]]
            ["key":["v1","v2"]]            | ["key":"v3"]                     | ["key": ["v1","v2","v3"]]
            ["key":["subkey":["v1","v2"]]] | ["key":["subkey":"v3"]]          | ["key": ["subkey":["v1","v2","v3"]]]
            ["key":["subkey":["v1":"v2"]]] | ["key":["subkey":["v3":"v4"]]]   | ["key": ["subkey":["v1":"v2","v3":"v4"]]]
    }

    def "should created map structure"() {
        expect:
            conv.createNestedMapStructure(map, keys, type) == newmap
        where:
            map                            | keys                             | type                           | newmap
            [:]                            | ["key1", "key2"]                 | "foo"                          | ["key1":["key2":"foo"]]
            [:]                            | ["key1", "key2"]                 | []                             | ["key1":["key2":[]]]
            [:]                            | ["key1", "key2","key3"]          | []                             | ["key1":["key2":["key3":[]]]]
            ["k1":["k2":"v1"]]             | ["k1", "k2"]                     | []                             | ["k1":["k2":["v1"]]]
            ["k1":["k2":"v1"]]             | ["k1", "k2", "k3"]               | []                             | ["k1":["k2":"v1","k3":[]]]
            ["k1":["k2":["k3":[]]]]        | ["k1", "k2", "k3"]               | []                             | ["k1":["k2":["k3":[]]]]
            ["k1":["k2":["k3":["foo"]]]]   | ["k1", "k2", "k3"]               | []                             | ["k1":["k2":["k3":["foo"]]]]
    }

    /*
     * Utility
     */
    def loadJson(jsonFile) {
        log.info("file: marc2jsonld/$jsonFile")
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc2jsonld/"+jsonFile)
        log.trace("stream: $is")
        return mapper.readValue(is, Map)
    }

    def cleanIsbn(jsonMarc) {
        def subA = jsonMarc?.get("subfields")[0]?.get("a").split(" ")
        def cleanedIsbn = subA[0].replaceAll("[^\\d]", "")
        def remainder = ""
        if (subA.length > 1) {
            remainder = " " + subA[1]
        }
        jsonMarc["subfields"][0]["a"] = cleanedIsbn + remainder
        return jsonMarc
    }
}
