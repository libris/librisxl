package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class Marc2JsonLDConverterSpec extends Specification {

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
            marc                                                                              | jsonld
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"]]]                     | ["preferredNameForThePerson" : "Svensson, Sven", "surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson"]
            ["ind1":"0","ind2":" ","subfields":[["a": "E-type"]]]                             | ["preferredNameForThePerson" : "E-type", "name":"E-type"]
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["d": "1952-"]]]     | ["preferredNameForThePerson" : "Svensson, Sven","surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson", "dateOfBirth":["@type":"year","@value":"1952"]]
            ["ind1":"1","ind2":" ","subfields":[["a": "Nilsson, Nisse"], ["d": "1948-2010"]]] | ["preferredNameForThePerson" : "Nilsson, Nisse","surname":"Nilsson", "givenName":"Nisse", "name": "Nisse Nilsson", "dateOfBirth":["@type":"year","@value":"1948"], "dateOfDeath":["@type":"year","@value":"2010"]]
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]       | [(Marc2JsonLDConverter.RAW_LABEL):["100":["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]]]
    }

    def "should map multiple authors"() {
        expect:
            conv.createJson(new URI("/bib/1234"), marc)["describes"] == jsonld
        where:
            marc                                                                                                                               | jsonld
            ["fields":[["100":["ind1":"1","subfields":[["a": "Svensson, Sven"]]]],["700":["ind1":"1","subfields":[["a":"Karlsson, Karl,"]]]]]] | ["expressionManifested":["authorList":[["preferredNameForThePerson" : "Svensson, Sven", "surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson"],["preferredNameForThePerson" : "Karlsson, Karl", "surname":"Karlsson", "givenName":"Karl", "name": "Karl Karlsson"]]]]
    }

    def "should map title"() {
        expect:
            conv.mapDefault("245", marc) == jsonld
        where:
            marc                                                                                            | jsonld
            [ "ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"]]]               | ["titleProper" : "Bokens titel", "statementOfResponsibilityRelatingToTitleProper" : "Kalle Kula"]
            [ "ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"],["z":"foo"]]]    | [(Marc2JsonLDConverter.RAW_LABEL):["245":[ "ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel", "c": "Kalle Kula", "z":"foo"]]]]]
}

    def "should map isbn"() {
        expect:
            conv.mapIsbn(marc) == jsonld
            vnoc.mapIsbn(jsonld) == cleanIsbn(marc)
        where:
            marc                                                                               | jsonld
            ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6 (inb.)"]]]               | ["isbn":"9100563226", "isbnRemainder": "(inb.)"]
            ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"]]]                      | ["isbn":"9100563226"]
            ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6 (inb.)"], ["c":"310:00"]]] | ["isbn":"9100563226", "isbnRemainder": "(inb.)", "termsOfAvailability":["literal":"310:00"]]
            ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["z":"foo"]]]           | [(Marc2JsonLDConverter.RAW_LABEL):["020":["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["z":"foo"]]]]]
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
