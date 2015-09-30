package whelk.util

import org.apache.commons.io.IOUtils
import spock.lang.Specification
import org.codehaus.jackson.map.*
import whelk.JsonLd


class JsonLdSpec extends Specification {

    def mapper

    def setup() {
        mapper = new ObjectMapper()
    }

    def "should frame flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("1_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("1_framed.jsonld"), Map)
        when:
        def resultJson = JsonLd.frame("/bib/13531679", flatJson)
        then:
        resultJson.get("about").get("identifier")[0].get("identifierValue") == "9789174771107"
        resultJson.get("about").get("genre")[0].get("prefLabel") == "E-böcker"
        resultJson.equals(framedJson)

    }

    def "should flatten framed jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("1_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("1_framed.jsonld"), Map)
        when:
        def resultJson = JsonLd.flatten(framedJson)
        then:
        flatJson.size() == resultJson.size()
    }

    def "should detect flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("1_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("1_framed.jsonld"), Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    static String loadJsonLdFile(String fileName) {
        InputStream is = JsonLdSpec.class.getClassLoader().getResourceAsStream(fileName)
        return IOUtils.toString(is, "UTF-8")
    }
}
