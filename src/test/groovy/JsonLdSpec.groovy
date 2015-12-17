package whelk.util

import com.google.common.base.Charsets
import org.apache.commons.io.IOUtils
import spock.lang.Specification
import org.codehaus.jackson.map.*
import whelk.Document
import whelk.JsonLd


class JsonLdSpec extends Specification {

    static final ObjectMapper mapper = new ObjectMapper()
    static List<String> flatfiles = IOUtils.readLines(JsonLdSpec.class.getClassLoader().getResourceAsStream("flatfiles/"), Charsets.UTF_8);
    static List<String> describedflatfiles = IOUtils.readLines(JsonLdSpec.class.getClassLoader().getResourceAsStream("describedflatfiles/"), Charsets.UTF_8);
    static List<String> framefiles = IOUtils.readLines(JsonLdSpec.class.getClassLoader().getResourceAsStream("framefiles/"), Charsets.UTF_8);

    def "should frame described flat jsonld"() {
        expect:
        JsonLd.frame(ids, flatJson).equals(framedJson)
        where:
        ids << describedflatfiles.collect { "/"+it.replace("_flat.jsonld", "").replace("_", "/") }
        flatJson << describedflatfiles.collect { mapper.readValue(loadJsonLdFile("describedflatfiles/${it}"), Map) }
        framedJson << framefiles.collect { mapper.readValue(loadJsonLdFile("framefiles/${it}"), Map) }
    }

    def "should frame flat jsonld"() {
        expect:
        JsonLd.frame(ids, flatJson).equals(framedJson)
        where:
        ids << flatfiles.collect { "/"+it.replace("_flat.jsonld", "").replace("_", "/") }
        flatJson << flatfiles.collect { mapper.readValue(loadJsonLdFile("flatfiles/${it}"), Map) }
        framedJson << flatfiles.collect { mapper.readValue(loadJsonLdFile("framefiles/${it.replace("_flat.jsonld", "_framed.jsonld")}"), Map) }
    }


    def "should flatten framed jsonld"() {
        expect:
        JsonLd.flatten(framedJson).equals(flatJson)
        where:
        flatJson << describedflatfiles.collect { mapper.readValue(loadJsonLdFile("describedflatfiles/${it}"), Map) }
        framedJson << framefiles.collect { mapper.readValue(loadJsonLdFile("framefiles/${it}"), Map) }
    }

    def "should detect flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("describedflatfiles/bib_13531679_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("framefiles/bib_13531679_framed.jsonld"), Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    static Map descriptionDocument = [
            "descriptions": [
                    "entry": [
                            "@id": "/qowiudhqw",
                            "name": "foo"
                    ],
                    "items": [
                            ["@id":"/qowiudhqw#it"]
                    ]
            ]
    ]

    def "should retrieve actual URI from @id in document"() {
        given:
        def data = new HashMap(descriptionDocument)
        and:
        data['descriptions']['entry']['@id'] = "http://id.kb.se/foo/bar"
        when:
        URI uri = JsonLd.findRecordURI(data)
        then:
        uri.toString() == "https://libris.kb.se/qowiudhqw"
        and:
        uri.toString() == "http://id.kb.se/foo/bar"

    }

    static String loadJsonLdFile(String fileName) {
        InputStream is = JsonLdSpec.class.getClassLoader().getResourceAsStream(fileName)
        return IOUtils.toString(is, "UTF-8")
    }




}
