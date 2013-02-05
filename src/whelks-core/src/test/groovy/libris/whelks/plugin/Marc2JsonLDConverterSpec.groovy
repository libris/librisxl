package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class Marc2JsonLDConverterSpec extends Specification {

    def conv = new Marc2JsonLDConverter()
    def mapper = new ObjectMapper()

    def "should map title"() {
        expect:
            conv.createJson(new URI(uri), loadJson(injson)) == loadJson(outjson)
        where:
            uri                     | injson                  | outjson
            "/bib/13531679"         | "in/13531679.json"      | "expected/13531679.json"
            "/bib/7149593"          | "in/7149593.json"       | "expected/7149593.json"
    }

    def loadJson(jsonFile) {
        log.info("file: marc2jsonld/$jsonFile")
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc2jsonld/"+jsonFile)
        return mapper.readValue(is, Map)
    }
}
