package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class Marc2JsonLDConverterSpec extends Specification implements Marc2JsonConstants {

    def mapper = new ObjectMapper()
    def conv = new Marc2JsonLDConverter()
    def vnoc = new JsonLD2MarcConverter()

    def "should map document"() {
        expect:
            conv.createJson(new URI(uri), loadJson(injson)) == loadJson(outjson)
            vnoc.mapDocument(loadJson(outjson)) == loadJson(injson)
        where:
            uri                     | injson                      | outjson
            "/bib/7149593"          | "in/bib/7149593.json"       | "expected/bib/7149593.json"
    }

    def "should map author"() {
        expect:
            conv.mapPerson("100", marc) == jsonld
            vnoc.mapPerson(jsonld) == marc
        where:
            marc          | jsonld
            AUTHOR_MARC_0 | AUTHOR_LD_0
            AUTHOR_MARC_1 | AUTHOR_LD_1
            AUTHOR_MARC_2 | AUTHOR_LD_2
            AUTHOR_MARC_3 | AUTHOR_LD_3
            AUTHOR_MARC_4 | AUTHOR_LD_4
    }

    def "should map multiple authors"() {
        expect:
            conv.createJson(new URI("/bib/1234"), marc)["describes"]["expressionManifested"] == jsonld
        where:
            marc                | jsonld
             AUTHOR_MULT_MARC_0 | AUTHOR_MULT_LD_0
    }

    def "should map title"() {
        expect:
            conv.mapDefault("245", marc) == jsonld
            vnoc.mapDefault(jsonld) == marc
        where:
            marc            | jsonld
            TITLE_MARC_0    | TITLE_LD_0
            TITLE_MARC_1    | TITLE_LD_1
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

    def "nbsi pam dluohs"() {
        expect:
            vnoc.mapIsbn(jsonld) == marc
        where:
            marc                |jsonld
            CLEANED_ISBN_MARC_0 | ISBN_LD_0
            CLEANED_ISBN_MARC_1 | ISBN_LD_1
            CLEANED_ISBN_MARC_2 | ISBN_LD_2
            CLEANED_ISBN_MARC_3 | ISBN_LD_3
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
    }

    /*
     * Utility
     */
    def loadJson(jsonFile) {
        log.info("file: marc2jsonld/$jsonFile")
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc2jsonld/"+jsonFile)
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
