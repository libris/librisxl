package whelk.util

import com.google.common.base.Charsets
import org.apache.commons.io.IOUtils
import spock.lang.Specification
import org.codehaus.jackson.map.*
import whelk.Document
import whelk.JsonLd
import whelk.exception.ModelValidationException


class JsonLdSpec extends Specification {

    static final ObjectMapper mapper = new ObjectMapper()
    static List<String> flatfiles = IOUtils.readLines(JsonLdSpec.class.getClassLoader().getResourceAsStream("flatfiles/"), Charsets.UTF_8);
    static List<String> describedflatfiles = IOUtils.readLines(JsonLdSpec.class.getClassLoader().getResourceAsStream("describedflatfiles/"), Charsets.UTF_8);
    static List<String> framefiles = IOUtils.readLines(JsonLdSpec.class.getClassLoader().getResourceAsStream("framefiles/"), Charsets.UTF_8);

    def "should get id map"() {
        expect:
        JsonLd.getIdMap(['@graph': items]).keySet() == ids as Set
        where:
        ids                     | items
        ['/some', '/other']     | [['@id': '/some'], ['@id': '/other']]
        ['/some', '/other']     | [['@id': '/some'], ['@graph': ['@id': '/other']]]
    }

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

    def "should preserve unframed json"() {
        expect:
        JsonLd.flatten(mundaneJson).equals(mundaneJson)
        where:
        mundaneJson = ["data":"foo","sameAs":"/some/other"]
    }

    def "should detect flat jsonld"() {
        given:
        def flatJson = mapper.readValue(loadJsonLdFile("describedflatfiles/bib_13531679_flat.jsonld"), Map)
        def framedJson = mapper.readValue(loadJsonLdFile("framefiles/bib_13531679_framed.jsonld"), Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    def "should retrieve actual URI from @id in document"() {
        when:
        URI uri1 = JsonLd.findRecordURI(["descriptions": ["entry": ["@id": "/qowiudhqw", "name": "foo"], "items": [["@id":"/qowiudhqw#it"]]]])
        URI uri2 = JsonLd.findRecordURI(["descriptions": ["entry": ["@id": "http://id.kb.se/foo/bar", "name": "foo"], "items": [["@id":"/qowiudhqw#it"]]]])
        URI uri3 = JsonLd.findRecordURI(["data":"foo","sameAs":"/some/other"])
        then:
        uri1.toString() == Document.BASE_URI.toString() + "qowiudhqw"
        uri2.toString() == "http://id.kb.se/foo/bar"
        uri3 == null
    }

    def "should find database id from @id in document"() {
        when:
        String s1 = JsonLd.findIdentifier(["descriptions": ["entry": ["@id": "/qowiudhqw", "name": "foo"], "items": [["@id":"/qowiudhqw#it"]]]])
        String s2 = JsonLd.findIdentifier(["descriptions": ["entry": ["@id": "http://id.kb.se/foo/bar", "name": "foo"], "items": [["@id":"/qowiudhqw#it"]]]])
        String s3 = JsonLd.findIdentifier(["descriptions": ["entry": ["@id": "https://libris.kb.se/qowiudhqw", "name": "foo"], "items": [["@id":"/qowiudhqw#it"]]]])
        String s4 = JsonLd.findIdentifier(["descriptions": ["entry": ["@id": Document.BASE_URI.toString() + "qowiudhqw", "name": "foo"], "items": [["@id":"/qowiudhqw#it"]]]])
        then:
        s1 == "qowiudhqw"
        s2 == "http://id.kb.se/foo/bar"
        s3 == "https://libris.kb.se/qowiudhqw"
        s4 == "qowiudhqw"
    }

    def "should validate model"() {
        when:
        boolean valid1 = JsonLd.validateItemModel(new Document("foo", ["@type": "HeldMaterial", "heldBy": ["@type":"Organization", "notation":"SEK"],"holdingFor":["@id": "https://libris.kb.se/foobar"]], ["collection":"hold"]))
        boolean valid2, valid3, valid4 = true
        try {
            valid2 = JsonLd.validateItemModel(new Document("foo", ["@type": "BadHeldMaterial", "heldBy": ["@type":"Organization", "notation":"SEK"],"holdingFor":["@id": "https://libris.kb.se/foobar"]], ["collection":"hold"]))
        } catch (ModelValidationException mve) {
            valid2 = false
        }
        try {
            valid3 = JsonLd.validateItemModel(new Document("foo", ["@type": "HeldMaterial", "heldBy": ["@type":"Organization", "notation":"SEK"]], ["collection":"hold"]))
        } catch (ModelValidationException mve) {
            valid3 = false
        }
        try {
            valid4 =JsonLd.validateItemModel(new Document("foo", ["@type": "HeldMaterial","holdingFor":["@id": "https://libris.kb.se/foobar"]], ["collection":"hold"]))
        } catch (ModelValidationException mve) {
            valid4 = false
        }
        then:
        valid1 == true
        valid2 == false
        valid3 == false
        valid4 == false
    }

    static String loadJsonLdFile(String fileName) {
        InputStream is = JsonLdSpec.class.getClassLoader().getResourceAsStream(fileName)
        return IOUtils.toString(is, "UTF-8")
    }




}
