package whelk.converter

import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class JsonLdTermReducerSpec extends Specification {

    static testCases = [
        [

            source: [
                "@type": "CreativeWork",
                "instanceTitle": [
                    "titleValue": "The Title"
                ]
            ],
            result: [
                "@type": "CreativeWork",
                "title": "The Title",
                "instanceTitle": [
                    "titleValue": "The Title"
                ]
            ],

            source: [
                "identifier": [
                    ["identifierValue": "0"],
                    ["identifierScheme": ["@id": "/def/identifiers/isbn"],
                     "identifierValue": "00-0-000000-0"]
                ]
            ],
            result: [
                "identifier": [
                    ["identifierValue": "0"],
                    ["identifierScheme": ["@id": "/def/identifiers/isbn"],
                     "identifierValue": "00-0-000000-0"]
                ],
                "isbn": ["0000000000"]
            ],

        ]
    ]

    def "should add reduced terms"() {
        given:
        def filter = new JsonLdTermReducer()
        when:
        def result = filter.doFilter(tc.source, "bib")
        then:
        result == tc.result
        where:
        tc << testCases
    }

}
