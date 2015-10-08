package whelk.util

import com.google.common.base.Charsets
import org.apache.commons.io.IOUtils
import spock.lang.Specification
import org.codehaus.jackson.map.*
import whelk.JsonLd


class JsonLdSpec extends Specification {

    static final ObjectMapper mapper = new ObjectMapper()
    static List<String> flatfiles, framefiles

    def setup() {
        flatfiles = IOUtils.readLines(this.getClass().getClassLoader().getResourceAsStream("flatfiles/"), Charsets.UTF_8);
        framefiles = IOUtils.readLines(this.getClass().getClassLoader().getResourceAsStream("framefiles/"), Charsets.UTF_8);
    }

    def "should frame flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("flatfiles/bib_13531679_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("framefiles/bib_13531679_framed.jsonld"), Map)
        when:
        def resultJson = JsonLd.frame("/bib/13531679", flatJson)
        then:
        resultJson.get("about").get("identifier")[0].get("identifierValue") == "9789174771107"
        resultJson.get("about").get("genre")[0].get("prefLabel") == "E-böcker"
        resultJson.equals(framedJson)

    }


    def "test many files"() {
        expect:
        JsonLd.frame(ids, flatJson).equals(framedJson)
        where:

        ids << flatfiles.collect { "/"+it.replace("_flat.jsonld", "").replace("_", "/") }
        flatJson << flatfiles.collect { mapper.readValue(loadJsonLdFile("flatfiles/${it}"), Map) }
        framedJson << framefiles.collect { mapper.readValue(loadJsonLdFile("framefiles/${it}"), Map) }

    }



    def "should flatten framed jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("flatfiles/bib_13531679_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("framefiles/bib_13531679_framed.jsonld"), Map)
        when:
        def resultJson = JsonLd.flatten(framedJson)
        then:
        flatJson.size() == resultJson.size()
    }

    def "should detect flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("flatfiles/bib_13531679_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("framefiles/bib_13531679_framed.jsonld"), Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    static String loadJsonLdFile(String fileName) {
        InputStream is = JsonLdSpec.class.getClassLoader().getResourceAsStream(fileName)
        return IOUtils.toString(is, "UTF-8")
    }


}
