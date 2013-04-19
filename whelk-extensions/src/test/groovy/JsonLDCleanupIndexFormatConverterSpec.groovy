package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.basic.*
import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupIndexFormatConverterSpec extends Specification {

    def mapper = new ObjectMapper()
    def conv = new JsonLDCleanupIndexFormatConverter()

    def "should return cleaned date"() {
        given:
            def injson = mapper.readValue(testin, Map)
            def indoc = new BasicDocument().withData(mapper.writeValueAsBytes(injson)).withContentType("application/ld+json").withIdentifier("bib/1527219")
        expect:
            conv.doConvert(indoc).get(0).getDataAsString() == cleanedout            
        where:
            testin                                              |   cleanedout
            "{\"about\":{\"dateOfPublication\":\"[1978]\"}}"    |   "{\"about\":{\"dateOfPublication\":\"1978\"}}"
    }
}
