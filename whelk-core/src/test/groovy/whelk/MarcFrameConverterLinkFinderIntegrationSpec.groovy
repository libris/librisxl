package whelk

import spock.lang.Requires
import spock.lang.Specification
import whelk.component.PostgreSQLComponent

class MarcFrameConverterLinkFinderIntegrationSpec extends Specification {

    @Requires({ env.testsuite == 'integration' })
    def "should linkfind when given extraData"() {
        given:
        // TODO: put properties in test/integration properties.
        // This also depends on example data being loaded.
        def whelk = new Whelk(
                new PostgreSQLComponent('jdbc:postgresql:whelk'))
        whelk.loadCoreData()
        def converter = whelk.getMarcFrameConverter()

        and:
            def source = [
                "leader": "00824cam a2200229    r4500",
                "fields": [
                    ["001": "816913"],
                    [
                        "100": [
                            "subfields": [
                                ["a": "Jansson, Tove,"],
                                ["d": "1914-2001"]
                            ],
                            "ind1": "1",
                            "ind2": " "
                        ]
                    ],
                    [
                        "700": [
                            "subfields": [
                                ["a": "Jansson, Lars,"],
                                ["d": "1926-2000"]
                            ],
                            "ind1": "1",
                            "ind2": " "
                        ]
                    ]
                ]
            ]
        def extraData = [
            oaipmhSetSpecs: ['authority:191503', 'authority:191493']
        ]

        when:
        def result = converter.runConvert(source, "testsource", extraData)
        then:
        result['@graph'][2]['qualifiedAttribution'][pos]['agent']['@id'] == uri
        where:
        pos | uri
        0   | 'https://libris.kb.se/wt79bh6f2j46dtr'
        1   | 'https://libris.kb.se/khwz05v34cc8nck'
    }

}
