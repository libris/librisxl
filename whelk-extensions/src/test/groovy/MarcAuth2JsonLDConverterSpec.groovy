package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class MarcAuth2JsonLDConverterSpec extends Specification {
    def mapper = new ObjectMapper()
    /* TODO: convert to new test

    def conv = new MarcAuth2JsonLDConverter()

    def "should map person"() {
        log.info("### should map person")
        expect:
            conv.mapField(injson, code, loadJsonSubField(marcdoc,code), loadJson(marcdoc)) == result
        where:
            marcdoc               | injson  | code  | result
            //"in/auth/254498.json" | ["unknown":["fields":["100":["subfields":["a":"apa"]]]]]      | "100" | [:]
            "in/auth/254498.json" | [:]     | "100" | [:]
    }

    def "should map creator"() {
        log.info("### should map creator")
        expect:
            conv.mapField(injson, code, loadJsonSubField(marcdoc,code), loadJson(marcdoc)) == result
        where:
            marcdoc               | injson  | code  | result
            "in/auth/254498.json" | [:]     | "040" | ["domain":["creatingInstitution":"some value"]]
    }

    def "should map annotation"() {
        log.info("### should map annotation")
        expect:
            conv.mapField(injson, code, loadJsonSubField(marcdoc,code), loadJson(marcdoc)) == result
        where:
            marcdoc               | injson  | code  | result
            "in/auth/254498.json" | [:]     | "667" | ["domain":["creatingInstitution":"some value"]]
    }
    def "should map source"() {
        log.info("### should map source")
        expect:
            conv.mapField(injson, code, loadJsonSubField(marcdoc,code), loadJson(marcdoc)) == result
        where:
            marcdoc               | injson  | code  | result
            "in/auth/254498.json" | [:]     | "670" | ["domain":["creatingInstitution":"some value"]]
    }

    /*
     * Utility
     */
    def loadJson(jsonFile) {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc2jsonld/"+jsonFile)
        return mapper.readValue(is, Map) 
    }

    def loadJsonSubField(jsonFile, code) {
        def map = loadJson(jsonFile)
        def v
        for (field in map.fields) {
            field.each { key, value ->
                if (key == code) {
                    v = value
                }
            }
        }
        return v
    }
}
