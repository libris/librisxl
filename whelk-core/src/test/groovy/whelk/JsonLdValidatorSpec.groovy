package whelk

import spock.lang.Ignore
import spock.lang.Specification
import static whelk.JsonLdValidator.*

class JsonLdValidatorSpec extends Specification {
    Map vocabData = [
            "@graph": [
                    ["@id" : "https://id.kb.se/vocab/ConceptScheme"],
                    ["@id" : "https://id.kb.se/vocab/label"]
            ]
    ]

    Map contextData = ["@graph" : [], //Declare graph as repeatable
                       "@context" : [
                               [
                                       "@vocab" : "https://id.kb.se/vocab/",
                                       "labelByLang": [
                                               "@id": "label",
                                               "@container": "@language"
                                       ]
                               ]
                       ]
    ]

    def setupValidator() {
        from(new JsonLd(contextData, [:], vocabData, [""]))
    }

    def "key that exists in vocab should pass validation"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll([
                "label"      : "someValue",
                "labelByLang": [
                        "can-be-any": "someValue",
                ]
        ])
        then:
        assert errors.isEmpty()
    }

    def "key that does not exist in vocab should return missing definition error"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["SomeNoneKbvKey": "someValue"])
        then:
        assert errors.any {it.type == JsonLdValidator.Error.Type.MISSING_DEFINITION}    
    }

    def "key that does not exist in vocab but is LD key should pass validation"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["@list": "someValue"])
        then:
        assert errors.isEmpty()
    }

    def "language container must be object"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["@graph": [
                ["labelByLang": "value"],
                ["labelByLang": 1],
                ["labelByLang": ["s"]],
        ]])
        then:
        assert errors.every {it.type == JsonLdValidator.Error.Type.OBJECT_TYPE_EXPECTED }
        assert errors.size() == 3
    }
    
    def "language container cannot contain anything that is not a string or a list"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll([
                "labelByLang": [
                        "sv": [ "a": "b" ],
                        "de": 3,
                ]
        ])
        then:
        assert errors.every {it.type == JsonLdValidator.Error.Type.UNEXPECTED }
        assert errors.size() == 2
    }

    def "language container can contain strings or lists"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll([
                "labelByLang": [
                        "en": ["someValue"],
                        "fi": "aString"
                ]
        ])
        then:
        assert errors.every {it.type == JsonLdValidator.Error.Type.UNEXPECTED }
        assert errors.size() == 0
    }


    def "repeatable term should pass validation if declared as repeatable in context"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["@graph": [["@id": "value"]]])
        then:
        assert errors.isEmpty()
    }

    def "non-repeatable term should not pass validation if declared as repeatable in context"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["@graph": "graph2"])
        then:
        assert errors.any {it.type == JsonLdValidator.Error.Type.ARRAY_EXPECTED}    }

    def "data with nested graph should return nested graph error"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validate(["@graph": [["@id": "value"], ["@graph": []]]])
        then:
        assert errors.any {it.type == JsonLdValidator.Error.Type.NESTED_GRAPH}
    }

    def "data without nested graph should pass validation"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validate(["@graph": [["@id": "value"]]])
        then:
        assert errors.isEmpty()
    }

    def "should not validate list elements"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["@graph": ["a"]])
        then:
        assert errors.isEmpty()
    }

    @Ignore // Currently can't handle this case
    def "should validate numeric keys"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validateAll(["@graph": [["0" : "a"]]])
        then:
        assert errors.any {it.type == JsonLdValidator.Error.Type.MISSING_DEFINITION}
    }

    def "@id with valid IRI should pass validation"() {
        given:
        def validator = setupValidator()
        def errors = validator.validate(["@graph": [["@id": id]]])

        expect:
        assert errors.isEmpty()

        where:
        id << [
                "https://id.kb.se/term/sao/Marsvin%20som%20s%C3%A4llskapsdjur",
                "https://ja.wikipedia.org/wiki/モルモット",
                "//foo"
        ]
    }

    def "@id with invalid IRI should not pass validation"() {
        given:
        def validator = setupValidator()
        def errors = validator.validate(["@graph": [["@id": id]]])

        expect:
        assert errors.any {it.type == JsonLdValidator.Error.Type.INVALID_IRI}

        where:
        id << [
                "\"https://libris.kb.se/library/DIGI\"",
                "https://example.com/ bar",
                "[sfsdfsdf]",
        ]
    }
}
