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
            conv.map100(marc) == jsonld
        where:
            marc                                                                          | jsonld
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"]]]                 | ["authorName":"Sven Svensson"]
            ["ind1":"0","ind2":" ","subfields":[["a": "E-type"]]]                         | ["authorName":"E-type"]
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["d": "1952-"]]] | ["authorName":"Sven Svensson", "authorDate": "1952-^^xsd:date"]
            ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]   | ["authorName":"Sven Svensson"]
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
