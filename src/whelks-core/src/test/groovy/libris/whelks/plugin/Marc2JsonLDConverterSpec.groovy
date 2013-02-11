package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class Marc2JsonLDConverterSpec extends Specification {

    def mapper = new ObjectMapper()

    def "should map document"() {
        setup:
            def conv = new Marc2JsonLDConverter()
        expect:
            conv.createJson(new URI(uri), loadJson(injson)) == loadJson(outjson)
        where:
            uri                     | injson                  | outjson
            "/bib/13531679"         | "in/13531679.json"      | "expected/13531679.json"
            "/bib/7149593"          | "in/7149593.json"       | "expected/7149593.json"
    }

    def "should map author"() {
        setup:
            def conv = new Marc2JsonLDConverter()
        expect:
            conv.mapPerson("100", marc) == jsonld
        where:
            marc                                                                          | jsonld
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"]]]                 | ["authorList":[["preferredNameForThePerson" : "Svensson, Sven", "surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson"]]]
            ["ind1":"0","ind2":" ","subfields":[["a": "E-type"]]]                         | ["authorList":[["preferredNameForThePerson" : "E-type", "name":"E-type"]]]
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["d": "1952-"]]] | ["authorList":[["preferredNameForThePerson" : "Svensson, Sven","surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson", "dateOfBirth":["@type":"year","@value":"1952"]]]]
            ["ind1":"1","ind2":" ","subfields":[["a": "Nilsson, Nisse"], ["d": "1948-2010"]]] | ["authorList":[["preferredNameForThePerson" : "Nilsson, Nisse","surname":"Nilsson", "givenName":"Nisse", "name": "Nisse Nilsson", "dateOfBirth":["@type":"year","@value":"1948"], "dateOfDeath":["@type":"year","@value":"2010"]]]]
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]   | ["raw":["100":["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]]]
    }

    def "should merge maps"() {
        setup: 
            def conv = new Marc2JsonLDConverter()
        expect:
            conv.mergeMap(o, n) == m
        where:
            o                   | n                | m
            ["foo":"bar"]       | ["foo":"beer"]   | ["foo": ["bar", "beer"]]
            ["key":["v1","v2"]] | ["key":"v3"]     | ["key": ["v1","v2","v3"]]
    }


    /*
     * Utility
     */
    def loadJson(jsonFile) {
        log.info("file: marc2jsonld/$jsonFile")
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc2jsonld/"+jsonFile)
        return mapper.readValue(is, Map)
    }
}
