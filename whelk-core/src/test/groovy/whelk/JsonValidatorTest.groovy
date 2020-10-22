package whelk

import spock.lang.Specification

class JsonValidatorSpec extends Specification {

    Map vocabData = [
            "@graph": [
                  ["@id" : "https://id.kb.se/vocab/ConceptScheme"]
            ]
    ]

    Map contextData = ["@graph" : [],
                       "@context" : [
                               ["@vocab" : "https://id.kb.se/vocab/"]
                       ]
    ]

    def setupValidator() {
        new JsonValidator(new JsonLd(contextData, [:], vocabData, [""]))
    }

    def "key that exists in vocab should be valid"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validate(["ConceptScheme" : "someValue"])
        then:
        assert errors.isEmpty()
    }

    def "key that does not exist in vocab should be invalid"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validate(["SomeNoneKbvKey" : "someValue"])
        then:
        assert !errors.isEmpty()
    }

    def "key that does not exist in vocab but is LD key should be valid"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validate(["@list": "someValue"])
        then:
        assert errors.isEmpty()
    }

    def "repeatable term should be valid"() {
        given:
        def validator = setupValidator()
        when:
        def errors = validator.validate(["@graph": "someValue"])
        then:
        assert errors.isEmpty()
    }

}
